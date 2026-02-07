package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MedelandenClusterSpec defines the desired state of a Medelanden broker cluster.
type MedelandenClusterSpec struct {
	// Replicas is the number of broker nodes in the cluster.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:default=3
	Replicas int32 `json:"replicas,omitempty"`

	// Image is the container image for the medelanden broker.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Storage configures persistent storage for each broker node.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`

	// Resources defines compute resource requirements for each broker node.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Streams defines the initial streams to create on the cluster.
	// +optional
	Streams []StreamSpec `json:"streams,omitempty"`

	// ImagePullPolicy defines the pull policy for the broker image.
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	// +kubebuilder:default="IfNotPresent"
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
}

// StorageSpec configures persistent storage for broker nodes.
type StorageSpec struct {
	// Size is the amount of storage to request per broker node.
	// +kubebuilder:default="1Gi"
	Size resource.Quantity `json:"size,omitempty"`

	// StorageClassName is the name of the StorageClass to use.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// StreamSpec defines a stream to create on the broker cluster at startup.
type StreamSpec struct {
	// Name is the stream identifier.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Subjects is the list of subject filters for message routing.
	// +kubebuilder:validation:MinItems=1
	Subjects []string `json:"subjects"`

	// ReplicationTarget is the desired number of replicas for this stream.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	// +optional
	ReplicationTarget int32 `json:"replicationTarget,omitempty"`
}

// MedelandenClusterStatus defines the observed state of MedelandenCluster.
type MedelandenClusterStatus struct {
	// Phase represents the current lifecycle phase of the cluster.
	// +optional
	Phase string `json:"phase,omitempty"`

	// ReadyReplicas is the number of broker nodes that are ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Endpoint is the client-facing service address for connecting to the cluster.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Conditions represent the latest available observations of the cluster's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mc
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MedelandenCluster is the Schema for the medelandenclusters API.
type MedelandenCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MedelandenClusterSpec   `json:"spec,omitempty"`
	Status MedelandenClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MedelandenClusterList contains a list of MedelandenCluster.
type MedelandenClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MedelandenCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MedelandenCluster{}, &MedelandenClusterList{})
}
