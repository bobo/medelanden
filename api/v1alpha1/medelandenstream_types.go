package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MedelandenStreamSpec defines the desired state of a Medelanden stream.
type MedelandenStreamSpec struct {
	// ClusterRef is the name of the MedelandenCluster this stream belongs to.
	// The cluster must exist in the same namespace.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ClusterRef string `json:"clusterRef"`

	// Subjects is the list of subject filters for message routing.
	// Supports NATS-style wildcards: * for single token, > for multi-token.
	// +kubebuilder:validation:MinItems=1
	Subjects []string `json:"subjects"`

	// MaxBytes is the maximum total size of messages in the stream (bytes).
	// Zero means unlimited.
	// +optional
	MaxBytes int64 `json:"maxBytes,omitempty"`

	// MaxAge is the maximum age of messages in the stream (e.g. "24h", "7d").
	// Empty means unlimited.
	// +optional
	MaxAge string `json:"maxAge,omitempty"`

	// MaxMsgs is the maximum number of messages in the stream.
	// Zero means unlimited.
	// +optional
	MaxMsgs int64 `json:"maxMsgs,omitempty"`

	// ReplicationTarget is the desired number of nodes replicating this stream.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	// +optional
	ReplicationTarget int32 `json:"replicationTarget,omitempty"`

	// FsyncPolicy determines when the WAL is fsynced to disk.
	// +kubebuilder:validation:Enum=none;interval;every
	// +kubebuilder:default="interval"
	// +optional
	FsyncPolicy string `json:"fsyncPolicy,omitempty"`

	// FsyncIntervalMs is the fsync interval in milliseconds when FsyncPolicy is "interval".
	// +kubebuilder:default=100
	// +optional
	FsyncIntervalMs int64 `json:"fsyncIntervalMs,omitempty"`

	// PlacementTags constrains which broker nodes may host this stream.
	// +optional
	PlacementTags []string `json:"placementTags,omitempty"`

	// PlacementCount is the number of nodes that must match placement tags.
	// +optional
	PlacementCount int32 `json:"placementCount,omitempty"`
}

// MedelandenStreamStatus defines the observed state of MedelandenStream.
type MedelandenStreamStatus struct {
	// Phase represents the current lifecycle phase of the stream.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Messages is the number of messages currently in the stream.
	// +optional
	Messages uint64 `json:"messages,omitempty"`

	// FirstSeq is the first sequence number in the stream.
	// +optional
	FirstSeq uint64 `json:"firstSeq,omitempty"`

	// LastSeq is the last sequence number in the stream.
	// +optional
	LastSeq uint64 `json:"lastSeq,omitempty"`

	// ConsumerCount is the number of consumers attached to the stream.
	// +optional
	ConsumerCount int32 `json:"consumerCount,omitempty"`

	// Conditions represent the latest available observations of the stream's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ms
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Messages",type=integer,JSONPath=`.status.messages`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MedelandenStream is the Schema for the medelandenstreams API.
type MedelandenStream struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MedelandenStreamSpec   `json:"spec,omitempty"`
	Status MedelandenStreamStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MedelandenStreamList contains a list of MedelandenStream.
type MedelandenStreamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MedelandenStream `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MedelandenStream{}, &MedelandenStreamList{})
}
