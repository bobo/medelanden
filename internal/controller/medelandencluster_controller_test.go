package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	medelandenv1alpha1 "medelanden/api/v1alpha1"
)

func TestReconcile_CreatesResources(t *testing.T) {
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

	reconciler := &MedelandenClusterReconciler{
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

	// Create a MedelandenCluster resource.
	cluster := &medelandenv1alpha1.MedelandenCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: medelandenv1alpha1.MedelandenClusterSpec{
			Replicas: 3,
			Image:    "medelanden:test",
			Storage: &medelandenv1alpha1.StorageSpec{
				Size: resource.MustParse("256Mi"),
			},
			Streams: []medelandenv1alpha1.StreamSpec{
				{
					Name:              "events",
					Subjects:          []string{"events.>"},
					ReplicationTarget: 2,
				},
			},
		},
	}

	if err := k8sClient.Create(ctx, cluster); err != nil {
		t.Fatalf("creating MedelandenCluster: %v", err)
	}

	// Wait for the StatefulSet to be created.
	var sts appsv1.StatefulSet
	if err := waitForResource(ctx, k8sClient, types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &sts); err != nil {
		t.Fatalf("waiting for StatefulSet: %v", err)
	}

	if *sts.Spec.Replicas != 3 {
		t.Errorf("expected 3 replicas, got %d", *sts.Spec.Replicas)
	}
	if sts.Spec.Template.Spec.Containers[0].Image != "medelanden:test" {
		t.Errorf("expected image medelanden:test, got %s", sts.Spec.Template.Spec.Containers[0].Image)
	}

	// Verify seeds argument contains all three pods.
	args := sts.Spec.Template.Spec.Containers[0].Args
	foundSeeds := false
	for _, arg := range args {
		if len(arg) > 8 && arg[:8] == "--seeds=" {
			foundSeeds = true
			if !containsAll(arg,
				"test-cluster-0.test-cluster-headless.default.svc.cluster.local:4223",
				"test-cluster-1.test-cluster-headless.default.svc.cluster.local:4223",
				"test-cluster-2.test-cluster-headless.default.svc.cluster.local:4223",
			) {
				t.Errorf("seeds arg missing expected entries: %s", arg)
			}
		}
	}
	if !foundSeeds {
		t.Error("--seeds argument not found in container args")
	}

	// Verify stream argument is present.
	foundStream := false
	for _, arg := range args {
		if len(arg) > 9 && arg[:9] == "--stream=" {
			foundStream = true
			if !contains(arg, "events") {
				t.Errorf("stream arg missing 'events': %s", arg)
			}
		}
	}
	if !foundStream {
		t.Error("--stream argument not found in container args")
	}

	// Wait for the headless Service to be created.
	var headlessSvc corev1.Service
	if err := waitForResource(ctx, k8sClient, types.NamespacedName{Name: "test-cluster-headless", Namespace: "default"}, &headlessSvc); err != nil {
		t.Fatalf("waiting for headless Service: %v", err)
	}
	if headlessSvc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("expected headless service (ClusterIP=None), got %s", headlessSvc.Spec.ClusterIP)
	}

	// Wait for the client Service to be created.
	var clientSvc corev1.Service
	if err := waitForResource(ctx, k8sClient, types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &clientSvc); err != nil {
		t.Fatalf("waiting for client Service: %v", err)
	}
	if clientSvc.Spec.ClusterIP == corev1.ClusterIPNone {
		t.Error("expected client service with ClusterIP, got None")
	}

	// Verify owner references are set on child resources.
	if len(sts.OwnerReferences) == 0 {
		t.Error("StatefulSet has no owner references")
	} else if sts.OwnerReferences[0].Kind != "MedelandenCluster" {
		t.Errorf("expected owner kind MedelandenCluster, got %s", sts.OwnerReferences[0].Kind)
	}

	if len(headlessSvc.OwnerReferences) == 0 {
		t.Error("headless Service has no owner references")
	}

	if len(clientSvc.OwnerReferences) == 0 {
		t.Error("client Service has no owner references")
	}

	// Verify status is updated.
	var updated medelandenv1alpha1.MedelandenCluster
	if err := waitForCondition(ctx, k8sClient, types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("waiting for status conditions: %v", err)
	}

	if updated.Status.Endpoint == "" {
		t.Error("expected endpoint to be set in status")
	}
}

func waitForResource(ctx context.Context, c client.Client, key types.NamespacedName, obj client.Object) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.Get(ctx, key, obj); err == nil {
				return nil
			}
		}
	}
}

func waitForCondition(ctx context.Context, c client.Client, key types.NamespacedName, cluster *medelandenv1alpha1.MedelandenCluster) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.Get(ctx, key, cluster); err != nil {
				continue
			}
			if len(cluster.Status.Conditions) > 0 {
				return nil
			}
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func containsAll(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if !contains(s, sub) {
			return false
		}
	}
	return true
}
