package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	medelandenv1alpha1 "medelanden/api/v1alpha1"
)

func TestStreamReconcile_SetsConditions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start envtest API server.
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	defer testEnv.Stop()

	// Register schemes.
	if err := medelandenv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		t.Fatalf("adding scheme: %v", err)
	}

	// Start manager with our controller.
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatalf("creating manager: %v", err)
	}

	reconciler := &MedelandenStreamReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		t.Fatalf("setting up controller: %v", err)
	}

	mgrCtx, mgrCancel := context.WithCancel(ctx)
	defer mgrCancel()
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			t.Errorf("manager exited with error: %v", err)
		}
	}()

	k8sClient := mgr.GetClient()

	// Create a MedelandenStream resource referencing a non-existent cluster.
	stream := &medelandenv1alpha1.MedelandenStream{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-stream",
			Namespace: "default",
		},
		Spec: medelandenv1alpha1.MedelandenStreamSpec{
			ClusterRef:        "non-existent-cluster",
			Subjects:          []string{"events.>"},
			ReplicationTarget: 2,
		},
	}

	if err := k8sClient.Create(ctx, stream); err != nil {
		t.Fatalf("creating MedelandenStream: %v", err)
	}

	// Wait for status conditions to be set (should be Pending/ClusterNotFound).
	var updated medelandenv1alpha1.MedelandenStream
	if err := waitForStreamCondition(ctx, k8sClient, types.NamespacedName{Name: "test-stream", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("waiting for stream conditions: %v", err)
	}

	if updated.Status.Phase != "Pending" {
		t.Errorf("expected phase Pending, got %s", updated.Status.Phase)
	}

	if len(updated.Status.Conditions) == 0 {
		t.Fatal("expected at least one condition")
	}

	readyCond := findCondition(updated.Status.Conditions, "Ready")
	if readyCond == nil {
		t.Fatal("expected Ready condition")
	}
	if readyCond.Status != metav1.ConditionFalse {
		t.Errorf("expected Ready=False, got %s", readyCond.Status)
	}
	if readyCond.Reason != "ClusterNotFound" {
		t.Errorf("expected reason ClusterNotFound, got %s", readyCond.Reason)
	}
}

func waitForStreamCondition(ctx context.Context, c client.Client, key types.NamespacedName, stream *medelandenv1alpha1.MedelandenStream) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.Get(ctx, key, stream); err != nil {
				continue
			}
			if len(stream.Status.Conditions) > 0 {
				return nil
			}
		}
	}
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
