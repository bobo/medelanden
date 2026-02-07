package controller

import (
	"context"
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
	consumerFinalizerName = "medelanden.io/consumer-cleanup"
	consumerRequeueDelay  = 10 * time.Second
)

// MedelandenConsumerReconciler reconciles a MedelandenConsumer object.
type MedelandenConsumerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=medelanden.io,resources=medelandenconsumers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=medelanden.io,resources=medelandenconsumers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=medelanden.io,resources=medelandenconsumers/finalizers,verbs=update

// Reconcile ensures the desired state of a MedelandenConsumer matches the broker's
// actual state. It creates or deletes consumers on the referenced cluster by
// connecting to its client endpoint.
func (r *MedelandenConsumerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Fetch the MedelandenConsumer resource.
	var consumer medelandenv1alpha1.MedelandenConsumer
	if err := r.Get(ctx, req.NamespacedName, &consumer); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Look up the referenced MedelandenCluster.
	var cluster medelandenv1alpha1.MedelandenCluster
	clusterKey := client.ObjectKey{Name: consumer.Spec.ClusterRef, Namespace: consumer.Namespace}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if errors.IsNotFound(err) {
			return r.setConsumerCondition(ctx, &consumer, "Pending", "ClusterNotFound",
				fmt.Sprintf("Cluster %q not found", consumer.Spec.ClusterRef))
		}
		return ctrl.Result{}, err
	}

	// 3. Ensure cluster is ready before proceeding.
	if cluster.Status.Phase != "Running" {
		return r.setConsumerCondition(ctx, &consumer, "Pending", "ClusterNotReady",
			fmt.Sprintf("Cluster %q is %s, waiting for Running", consumer.Spec.ClusterRef, cluster.Status.Phase))
	}

	endpoint := cluster.Status.Endpoint

	// 4. Handle deletion via finalizer.
	if !consumer.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&consumer, consumerFinalizerName) {
			log.Info("Deleting consumer from broker", "consumer", consumer.Name, "stream", consumer.Spec.Stream)
			if err := r.deleteConsumerFromBroker(endpoint, consumer.Spec.Stream, consumer.Name); err != nil {
				log.Error(err, "Failed to delete consumer from broker, proceeding with finalizer removal")
			}
			controllerutil.RemoveFinalizer(&consumer, consumerFinalizerName)
			if err := r.Update(ctx, &consumer); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// 5. Ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(&consumer, consumerFinalizerName) {
		controllerutil.AddFinalizer(&consumer, consumerFinalizerName)
		if err := r.Update(ctx, &consumer); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// 6. Create the consumer on the broker.
	if err := r.ensureConsumerOnBroker(endpoint, &consumer); err != nil {
		log.Error(err, "Failed to create consumer on broker")
		return r.setConsumerCondition(ctx, &consumer, "Failed", "CreateFailed", err.Error())
	}

	// 7. Fetch consumer info from broker and update status.
	state, err := r.getConsumerInfoFromBroker(endpoint, consumer.Spec.Stream, consumer.Name)
	if err != nil {
		log.Error(err, "Failed to get consumer info from broker")
		_, updateErr := r.setConsumerCondition(ctx, &consumer, "Active", "InfoUnavailable",
			"Consumer created but unable to fetch current state")
		if updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{RequeueAfter: consumerRequeueDelay}, nil
	}

	consumer.Status.Phase = "Active"
	consumer.Status.Watermark = state.Watermark
	consumer.Status.LateMessages = state.LateMessages
	consumer.Status.DedupCount = state.DedupCount
	consumer.Status.ObservedGeneration = consumer.Generation
	meta.SetStatusCondition(&consumer.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		ObservedGeneration: consumer.Generation,
		Reason:             "ConsumerActive",
		Message:            fmt.Sprintf("Consumer is active on stream %s", consumer.Spec.Stream),
	})

	if err := r.Status().Update(ctx, &consumer); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("Consumer reconciliation complete",
		"consumer", consumer.Name,
		"stream", consumer.Spec.Stream,
		"phase", consumer.Status.Phase,
	)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// setConsumerCondition updates the consumer status with the given phase and condition.
func (r *MedelandenConsumerReconciler) setConsumerCondition(ctx context.Context, consumer *medelandenv1alpha1.MedelandenConsumer, phase, reason, message string) (ctrl.Result, error) {
	consumer.Status.Phase = phase
	consumer.Status.ObservedGeneration = consumer.Generation

	condStatus := metav1.ConditionFalse
	if phase == "Active" {
		condStatus = metav1.ConditionTrue
	}

	meta.SetStatusCondition(&consumer.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             condStatus,
		ObservedGeneration: consumer.Generation,
		Reason:             reason,
		Message:            message,
	})

	if err := r.Status().Update(ctx, consumer); err != nil {
		return ctrl.Result{}, err
	}

	if phase == "Pending" {
		return ctrl.Result{RequeueAfter: consumerRequeueDelay}, nil
	}
	return ctrl.Result{}, fmt.Errorf("%s: %s", reason, message)
}

// ensureConsumerOnBroker connects to the broker and creates the consumer.
func (r *MedelandenConsumerReconciler) ensureConsumerOnBroker(endpoint string, consumer *medelandenv1alpha1.MedelandenConsumer) error {
	c, err := medelandenclient.Dial(endpoint,
		medelandenclient.WithReadTimeout(5*time.Second),
		medelandenclient.WithWriteTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("connecting to broker at %s: %w", endpoint, err)
	}
	defer c.Close()

	cfg := medelandenclient.ConsumerConfig{
		Name:             consumer.Name,
		Stream:           consumer.Spec.Stream,
		Ordering:         consumer.Spec.Ordering,
		WindowDuration:   consumer.Spec.WindowDuration,
		WatermarkTimeout: consumer.Spec.WatermarkTimeout,
		DedupKey:         consumer.Spec.DedupKey,
		LatePolicy:       consumer.Spec.LatePolicy,
		DeliverPolicy:    consumer.Spec.DeliverPolicy,
		SubjectFilter:    consumer.Spec.SubjectFilter,
	}

	err = c.CreateConsumer(cfg)
	if err != nil {
		if isAlreadyExistsError(err) {
			return nil
		}
		return fmt.Errorf("creating consumer %s: %w", consumer.Name, err)
	}
	return nil
}

// deleteConsumerFromBroker connects to the broker and deletes the consumer.
func (r *MedelandenConsumerReconciler) deleteConsumerFromBroker(endpoint, stream, name string) error {
	c, err := medelandenclient.Dial(endpoint,
		medelandenclient.WithReadTimeout(5*time.Second),
		medelandenclient.WithWriteTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("connecting to broker at %s: %w", endpoint, err)
	}
	defer c.Close()

	return c.DeleteConsumer(stream, name)
}

// getConsumerInfoFromBroker fetches the consumer state from the broker.
func (r *MedelandenConsumerReconciler) getConsumerInfoFromBroker(endpoint, stream, name string) (*medelandenclient.ConsumerState, error) {
	c, err := medelandenclient.Dial(endpoint,
		medelandenclient.WithReadTimeout(5*time.Second),
		medelandenclient.WithWriteTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("connecting to broker at %s: %w", endpoint, err)
	}
	defer c.Close()

	return c.ConsumerInfo(stream, name)
}

// SetupWithManager sets up the consumer controller with the Manager.
func (r *MedelandenConsumerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&medelandenv1alpha1.MedelandenConsumer{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)
}
