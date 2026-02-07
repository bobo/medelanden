package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	medelandenv1alpha1 "medelanden/api/v1alpha1"
	medelandenclient "medelanden/client"
)

const (
	streamFinalizerName = "medelanden.io/stream-cleanup"
	streamRequeueDelay  = 10 * time.Second
)

// MedelandenStreamReconciler reconciles a MedelandenStream object.
type MedelandenStreamReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=medelanden.io,resources=medelandenstreams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=medelanden.io,resources=medelandenstreams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=medelanden.io,resources=medelandenstreams/finalizers,verbs=update

// Reconcile ensures the desired state of a MedelandenStream matches the broker's
// actual state. It creates or deletes streams on the referenced cluster by
// connecting to its client endpoint.
func (r *MedelandenStreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Fetch the MedelandenStream resource.
	var stream medelandenv1alpha1.MedelandenStream
	if err := r.Get(ctx, req.NamespacedName, &stream); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Look up the referenced MedelandenCluster.
	var cluster medelandenv1alpha1.MedelandenCluster
	clusterKey := client.ObjectKey{Name: stream.Spec.ClusterRef, Namespace: stream.Namespace}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if errors.IsNotFound(err) {
			return r.setStreamCondition(ctx, &stream, "Pending", "ClusterNotFound",
				fmt.Sprintf("Cluster %q not found", stream.Spec.ClusterRef))
		}
		return ctrl.Result{}, err
	}

	// 3. Ensure cluster is ready before proceeding.
	if cluster.Status.Phase != "Running" {
		return r.setStreamCondition(ctx, &stream, "Pending", "ClusterNotReady",
			fmt.Sprintf("Cluster %q is %s, waiting for Running", stream.Spec.ClusterRef, cluster.Status.Phase))
	}

	endpoint := cluster.Status.Endpoint

	// 4. Handle deletion via finalizer.
	if !stream.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&stream, streamFinalizerName) {
			log.Info("Deleting stream from broker", "stream", stream.Name)
			if err := r.deleteStreamFromBroker(endpoint, stream.Name); err != nil {
				log.Error(err, "Failed to delete stream from broker, proceeding with finalizer removal")
			}
			controllerutil.RemoveFinalizer(&stream, streamFinalizerName)
			if err := r.Update(ctx, &stream); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// 5. Ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(&stream, streamFinalizerName) {
		controllerutil.AddFinalizer(&stream, streamFinalizerName)
		if err := r.Update(ctx, &stream); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// 6. Create the stream on the broker.
	if err := r.ensureStreamOnBroker(endpoint, &stream); err != nil {
		log.Error(err, "Failed to create stream on broker")
		return r.setStreamCondition(ctx, &stream, "Failed", "CreateFailed", err.Error())
	}

	// 7. Fetch stream info from broker and update status.
	state, err := r.getStreamInfoFromBroker(endpoint, stream.Name)
	if err != nil {
		log.Error(err, "Failed to get stream info from broker")
		// Stream was created but info fetch failed; set as Active and requeue.
		_, updateErr := r.setStreamCondition(ctx, &stream, "Active", "InfoUnavailable",
			"Stream created but unable to fetch current state")
		if updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{RequeueAfter: streamRequeueDelay}, nil
	}

	stream.Status.Phase = "Active"
	stream.Status.Messages = state.Messages
	stream.Status.FirstSeq = state.FirstSeq
	stream.Status.LastSeq = state.LastSeq
	stream.Status.ConsumerCount = int32(state.ConsumerCount)
	stream.Status.ObservedGeneration = stream.Generation
	meta.SetStatusCondition(&stream.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: stream.Generation,
		Reason:             "StreamActive",
		Message:            fmt.Sprintf("Stream is active with %d messages", state.Messages),
	})

	if err := r.Status().Update(ctx, &stream); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("Stream reconciliation complete",
		"stream", stream.Name,
		"messages", state.Messages,
		"phase", stream.Status.Phase,
	)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// setStreamCondition updates the stream status with the given phase and condition.
func (r *MedelandenStreamReconciler) setStreamCondition(ctx context.Context, stream *medelandenv1alpha1.MedelandenStream, phase, reason, message string) (ctrl.Result, error) {
	stream.Status.Phase = phase
	stream.Status.ObservedGeneration = stream.Generation

	condStatus := metav1.ConditionFalse
	if phase == "Active" {
		condStatus = metav1.ConditionTrue
	}

	meta.SetStatusCondition(&stream.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             condStatus,
		ObservedGeneration: stream.Generation,
		Reason:             reason,
		Message:            message,
	})

	if err := r.Status().Update(ctx, stream); err != nil {
		return ctrl.Result{}, err
	}

	if phase == "Pending" {
		return ctrl.Result{RequeueAfter: streamRequeueDelay}, nil
	}
	return ctrl.Result{}, fmt.Errorf("%s: %s", reason, message)
}

// ensureStreamOnBroker connects to the broker and creates the stream.
func (r *MedelandenStreamReconciler) ensureStreamOnBroker(endpoint string, stream *medelandenv1alpha1.MedelandenStream) error {
	c, err := medelandenclient.Dial(endpoint,
		medelandenclient.WithReadTimeout(5*time.Second),
		medelandenclient.WithWriteTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("connecting to broker at %s: %w", endpoint, err)
	}
	defer c.Close()

	cfg := medelandenclient.StreamConfig{
		Name:              stream.Name,
		Subjects:          stream.Spec.Subjects,
		MaxBytes:          stream.Spec.MaxBytes,
		MaxAge:            stream.Spec.MaxAge,
		MaxMsgs:           stream.Spec.MaxMsgs,
		ReplicationTarget: int(stream.Spec.ReplicationTarget),
		FsyncPolicy:       stream.Spec.FsyncPolicy,
	}

	err = c.CreateStream(cfg)
	if err != nil {
		// If stream already exists, that's fine.
		if isAlreadyExistsError(err) {
			return nil
		}
		return fmt.Errorf("creating stream %s: %w", stream.Name, err)
	}
	return nil
}

// deleteStreamFromBroker connects to the broker and deletes the stream.
func (r *MedelandenStreamReconciler) deleteStreamFromBroker(endpoint, name string) error {
	c, err := medelandenclient.Dial(endpoint,
		medelandenclient.WithReadTimeout(5*time.Second),
		medelandenclient.WithWriteTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("connecting to broker at %s: %w", endpoint, err)
	}
	defer c.Close()

	return c.DeleteStream(name)
}

// getStreamInfoFromBroker fetches the stream state from the broker.
func (r *MedelandenStreamReconciler) getStreamInfoFromBroker(endpoint, name string) (*medelandenclient.StreamState, error) {
	c, err := medelandenclient.Dial(endpoint,
		medelandenclient.WithReadTimeout(5*time.Second),
		medelandenclient.WithWriteTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("connecting to broker at %s: %w", endpoint, err)
	}
	defer c.Close()

	return c.StreamInfo(name)
}

// isAlreadyExistsError checks if the error indicates the resource already exists.
func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return containsSubstring(errMsg, "already exists")
}

// containsSubstring returns true if s contains substr.
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstr(s, substr)
}

func searchSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// streamConfigJSON is used only for debug/log purposes.
func streamConfigJSON(cfg medelandenclient.StreamConfig) string {
	b, _ := json.Marshal(cfg)
	return string(b)
}

// SetupWithManager sets up the stream controller with the Manager.
func (r *MedelandenStreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&medelandenv1alpha1.MedelandenStream{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)
}
