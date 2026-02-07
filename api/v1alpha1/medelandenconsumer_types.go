package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MedelandenConsumerSpec defines the desired state of a Medelanden consumer.
type MedelandenConsumerSpec struct {
	// ClusterRef is the name of the MedelandenCluster this consumer belongs to.
	// The cluster must exist in the same namespace.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	ClusterRef string `json:"clusterRef"`

	// Stream is the name of the stream this consumer reads from.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Stream string `json:"stream"`

	// Ordering determines how messages are sorted within windows.
	// +kubebuilder:default="producer_ts"
	// +optional
	Ordering string `json:"ordering,omitempty"`

	// WindowDuration is the duration of each time window (e.g. "2s", "500ms").
	// +kubebuilder:default="2s"
	// +optional
	WindowDuration string `json:"windowDuration,omitempty"`

	// WatermarkTimeout is how long to wait for a slow source before advancing the watermark.
	// +kubebuilder:default="5s"
	// +optional
	WatermarkTimeout string `json:"watermarkTimeout,omitempty"`

	// DedupKey specifies the fields used to deduplicate messages within a window.
	// Valid values: msg_id, subject, producer_ts, dedup_key, node_id.
	// +kubebuilder:default={"msg_id"}
	// +optional
	DedupKey []string `json:"dedupKey,omitempty"`

	// LatePolicy determines how messages arriving after their window closed are handled.
	// +kubebuilder:validation:Enum=drop;emit_unordered
	// +kubebuilder:default="drop"
	// +optional
	LatePolicy string `json:"latePolicy,omitempty"`

	// DeliverPolicy determines the starting position for a new consumer.
	// +kubebuilder:validation:Enum=new;all;by_time
	// +kubebuilder:default="new"
	// +optional
	DeliverPolicy string `json:"deliverPolicy,omitempty"`

	// SubjectFilter restricts consumption to a specific subject pattern.
	// +kubebuilder:default=">"
	// +optional
	SubjectFilter string `json:"subjectFilter,omitempty"`
}

// MedelandenConsumerStatus defines the observed state of MedelandenConsumer.
type MedelandenConsumerStatus struct {
	// Phase represents the current lifecycle phase of the consumer.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Watermark is the current watermark timestamp (producer_ts in nanoseconds).
	// +optional
	Watermark uint64 `json:"watermark,omitempty"`

	// LateMessages is the count of messages that arrived after their window closed.
	// +optional
	LateMessages uint64 `json:"lateMessages,omitempty"`

	// DedupCount is the count of deduplicated messages.
	// +optional
	DedupCount uint64 `json:"dedupCount,omitempty"`

	// Conditions represent the latest available observations of the consumer's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mcon
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Stream",type=string,JSONPath=`.spec.stream`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MedelandenConsumer is the Schema for the medelandenconsumers API.
type MedelandenConsumer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MedelandenConsumerSpec   `json:"spec,omitempty"`
	Status MedelandenConsumerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MedelandenConsumerList contains a list of MedelandenConsumer.
type MedelandenConsumerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MedelandenConsumer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MedelandenConsumer{}, &MedelandenConsumerList{})
}
