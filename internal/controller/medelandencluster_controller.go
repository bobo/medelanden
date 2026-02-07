package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	medelandenv1alpha1 "medelanden/api/v1alpha1"
)

const (
	finalizerName = "medelanden.io/cluster-cleanup"

	// Ports used by the medelanden broker.
	clientPort = 4222
	peerPort   = 4223
	httpPort   = 8080
)

// MedelandenClusterReconciler reconciles a MedelandenCluster object.
type MedelandenClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=medelanden.io,resources=medelandenclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=medelanden.io,resources=medelandenclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=medelanden.io,resources=medelandenclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete

// Reconcile reconciles the desired state of a MedelandenCluster with the actual
// cluster state. It creates or updates the headless Service, client Service, and
// StatefulSet for each MedelandenCluster resource.
func (r *MedelandenClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Fetch the MedelandenCluster resource.
	var cluster medelandenv1alpha1.MedelandenCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Handle deletion via finalizer.
	if !cluster.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cluster, finalizerName) {
			log.Info("Cleaning up cluster resources")
			controllerutil.RemoveFinalizer(&cluster, finalizerName)
			if err := r.Update(ctx, &cluster); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// 3. Ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(&cluster, finalizerName) {
		controllerutil.AddFinalizer(&cluster, finalizerName)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// 4. Set phase to Provisioning if not yet set.
	if cluster.Status.Phase == "" {
		cluster.Status.Phase = "Provisioning"
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "Provisioning",
			Message:            "Cluster is being provisioned",
		})
		if err := r.Status().Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 5. Reconcile child resources.
	if err := r.reconcileHeadlessService(ctx, &cluster); err != nil {
		return r.setFailedCondition(ctx, &cluster, "HeadlessServiceFailed", err)
	}
	if err := r.reconcileClientService(ctx, &cluster); err != nil {
		return r.setFailedCondition(ctx, &cluster, "ClientServiceFailed", err)
	}
	if err := r.reconcileStatefulSet(ctx, &cluster); err != nil {
		return r.setFailedCondition(ctx, &cluster, "StatefulSetFailed", err)
	}

	// 6. Read back the StatefulSet to determine ready replicas.
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKey{Name: cluster.Name, Namespace: cluster.Namespace}, &sts); err != nil {
		return ctrl.Result{}, err
	}

	cluster.Status.ReadyReplicas = sts.Status.ReadyReplicas
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Status.Endpoint = fmt.Sprintf("%s.%s.svc.cluster.local:%d", cluster.Name, cluster.Namespace, clientPort)

	if sts.Status.ReadyReplicas == cluster.Spec.Replicas {
		cluster.Status.Phase = "Running"
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: cluster.Generation,
			Reason:             "ClusterReady",
			Message:            fmt.Sprintf("All %d replicas are ready", cluster.Spec.Replicas),
		})
	} else {
		cluster.Status.Phase = "Provisioning"
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "WaitingForReplicas",
			Message:            fmt.Sprintf("%d/%d replicas ready", sts.Status.ReadyReplicas, cluster.Spec.Replicas),
		})
	}

	if err := r.Status().Update(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("Reconciliation complete",
		"replicas", cluster.Spec.Replicas,
		"ready", cluster.Status.ReadyReplicas,
		"phase", cluster.Status.Phase,
	)
	return ctrl.Result{}, nil
}

// setFailedCondition updates the cluster status with a failure condition.
func (r *MedelandenClusterReconciler) setFailedCondition(ctx context.Context, cluster *medelandenv1alpha1.MedelandenCluster, reason string, err error) (ctrl.Result, error) {
	cluster.Status.Phase = "Failed"
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            err.Error(),
	})
	_ = r.Status().Update(ctx, cluster)
	return ctrl.Result{}, err
}

// reconcileHeadlessService creates or updates the headless Service used for
// StatefulSet pod DNS discovery and peer-to-peer communication.
func (r *MedelandenClusterReconciler) reconcileHeadlessService(ctx context.Context, cluster *medelandenv1alpha1.MedelandenCluster) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-headless",
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		labels := clusterLabels(cluster)
		svc.Labels = labels
		svc.Spec = corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labels,
			Ports: []corev1.ServicePort{
				{Name: "client", Port: clientPort, TargetPort: intstr.FromInt32(clientPort)},
				{Name: "peer", Port: peerPort, TargetPort: intstr.FromInt32(peerPort)},
				{Name: "http", Port: httpPort, TargetPort: intstr.FromInt32(httpPort)},
			},
		}
		return controllerutil.SetControllerReference(cluster, svc, r.Scheme)
	})
	return err
}

// reconcileClientService creates or updates the ClusterIP Service used for
// client connections to the broker cluster.
func (r *MedelandenClusterReconciler) reconcileClientService(ctx context.Context, cluster *medelandenv1alpha1.MedelandenCluster) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		labels := clusterLabels(cluster)
		svc.Labels = labels
		svc.Spec = corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{Name: "client", Port: clientPort, TargetPort: intstr.FromInt32(clientPort)},
				{Name: "http", Port: httpPort, TargetPort: intstr.FromInt32(httpPort)},
			},
		}
		return controllerutil.SetControllerReference(cluster, svc, r.Scheme)
	})
	return err
}

// reconcileStatefulSet creates or updates the StatefulSet that runs the
// medelanden broker nodes.
func (r *MedelandenClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *medelandenv1alpha1.MedelandenCluster) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		labels := clusterLabels(cluster)
		replicas := cluster.Spec.Replicas

		headlessSvcName := cluster.Name + "-headless"
		seeds := buildSeedList(cluster.Name, headlessSvcName, cluster.Namespace, replicas)

		args := []string{
			"--bind=0.0.0.0:4222",
			"--peer-addr=0.0.0.0:4223",
			"--http=0.0.0.0:8080",
			"--data-dir=/data",
			"--seeds=" + seeds,
		}

		// Add initial stream definitions as args.
		for _, stream := range cluster.Spec.Streams {
			streamJSON, err := buildStreamJSON(stream)
			if err != nil {
				return fmt.Errorf("marshaling stream %q: %w", stream.Name, err)
			}
			args = append(args, "--stream="+streamJSON)
		}

		container := corev1.Container{
			Name:  "medelanden",
			Image: cluster.Spec.Image,
			Args:  args,
			Ports: []corev1.ContainerPort{
				{Name: "client", ContainerPort: clientPort},
				{Name: "peer", ContainerPort: peerPort},
				{Name: "http", ContainerPort: httpPort},
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/healthz",
						Port: intstr.FromInt32(httpPort),
					},
				},
				InitialDelaySeconds: 2,
				PeriodSeconds:       3,
			},
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/healthz",
						Port: intstr.FromInt32(httpPort),
					},
				},
				InitialDelaySeconds: 5,
				PeriodSeconds:       10,
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "data", MountPath: "/data"},
			},
		}

		if cluster.Spec.ImagePullPolicy != "" {
			container.ImagePullPolicy = cluster.Spec.ImagePullPolicy
		}

		if cluster.Spec.Resources != nil {
			container.Resources = *cluster.Spec.Resources
		}

		sts.Labels = labels
		sts.Spec = appsv1.StatefulSetSpec{
			ServiceName: headlessSvcName,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: int64Ptr(5),
					Containers:                    []corev1.Container{container},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "data"},
					Spec:       buildPVCSpec(cluster.Spec.Storage),
				},
			},
		}

		return controllerutil.SetControllerReference(cluster, sts, r.Scheme)
	})
	return err
}

// SetupWithManager sets up the controller with the Manager.
func (r *MedelandenClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&medelandenv1alpha1.MedelandenCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)
}

// clusterLabels returns the standard set of labels for all resources
// belonging to a MedelandenCluster.
func clusterLabels(cluster *medelandenv1alpha1.MedelandenCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "medelanden",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/managed-by": "medelanden-operator",
	}
}

// buildSeedList builds the comma-separated seed peer addresses for a
// StatefulSet of the given size. Each pod gets a stable DNS name:
// <name>-<ordinal>.<headless-svc>.<namespace>.svc.cluster.local:<peer-port>
func buildSeedList(name, headlessSvc, namespace string, replicas int32) string {
	seeds := make([]string, replicas)
	for i := int32(0); i < replicas; i++ {
		seeds[i] = fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d",
			name, i, headlessSvc, namespace, peerPort)
	}
	return strings.Join(seeds, ",")
}

// streamDef is the JSON structure expected by the medelanden --stream flag.
type streamDef struct {
	Name              string   `json:"name"`
	Subjects          []string `json:"subjects"`
	ReplicationTarget int32    `json:"replication_target"`
	FsyncPolicy       string   `json:"fsync_policy"`
	FsyncInterval     int64    `json:"fsync_interval"`
}

// buildStreamJSON marshals a StreamSpec into the JSON format expected by the
// medelanden broker's --stream flag.
func buildStreamJSON(s medelandenv1alpha1.StreamSpec) (string, error) {
	def := streamDef{
		Name:              s.Name,
		Subjects:          s.Subjects,
		ReplicationTarget: s.ReplicationTarget,
		FsyncPolicy:       "interval",
		FsyncInterval:     100_000_000, // 100ms in nanoseconds
	}
	b, err := json.Marshal(def)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// buildPVCSpec returns a PersistentVolumeClaimSpec from the storage config.
func buildPVCSpec(storage *medelandenv1alpha1.StorageSpec) corev1.PersistentVolumeClaimSpec {
	size := resource.MustParse("1Gi")
	if storage != nil && !storage.Size.IsZero() {
		size = storage.Size
	}

	spec := corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceStorage: size,
			},
		},
	}

	if storage != nil && storage.StorageClassName != nil {
		spec.StorageClassName = storage.StorageClassName
	}

	return spec
}

func int64Ptr(i int64) *int64 { return &i }
