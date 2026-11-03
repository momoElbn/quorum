package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// QuorumClusterSpec is what the user asks for.
type QuorumClusterSpec struct {
	// Replicas is the number of Raft nodes. Use an odd number: 3 nodes
	// survive 1 failure, 5 survive 2.
	//
	// It can't be changed after creation yet: adding or removing nodes
	// also needs Raft membership changes (POST /join and /remove), which
	// the operator doesn't do yet. The rule below makes the API server
	// reject the change, instead of leaving Raft with members that no
	// longer exist.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="replicas can't be changed yet (Raft membership changes aren't automated)"
	Replicas int32 `json:"replicas"`

	// Image is the quorum-server container image, e.g. "quorum-server:dev".
	// Changing it triggers a rolling update, one pod at a time.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// StorageSize is the size of each node's volume (Raft log, vote and
	// snapshots).
	//
	// It can't be changed after creation: a StatefulSet's
	// volumeClaimTemplates are immutable in Kubernetes.
	// +kubebuilder:default="1Gi"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storageSize can't be changed after creation"
	StorageSize resource.Quantity `json:"storageSize"`
}

// QuorumClusterPhase is a one-word summary of the cluster's health.
// +kubebuilder:validation:Enum=Pending;Running;Degraded;Unavailable
type QuorumClusterPhase string

const (
	// PhasePending: no pod is ready yet, for example right after creation.
	PhasePending QuorumClusterPhase = "Pending"
	// PhaseRunning: a leader is elected, and every node is ready and a
	// Raft member.
	PhaseRunning QuorumClusterPhase = "Running"
	// PhaseDegraded: a leader is elected, so reads and writes work, but
	// some node is down or not a member. One more failure may stop the
	// cluster.
	PhaseDegraded QuorumClusterPhase = "Degraded"
	// PhaseUnavailable: pods are running but there is no leader, usually
	// because a majority is down. Reads and writes fail.
	PhaseUnavailable QuorumClusterPhase = "Unavailable"
)

// Condition types reported in status.conditions.
const (
	// ConditionAvailable is True when a leader is elected, meaning the
	// cluster can serve reads and writes.
	ConditionAvailable = "Available"
	// ConditionDegraded is True when the cluster runs with fewer healthy
	// nodes than requested.
	ConditionDegraded = "Degraded"
)

// QuorumClusterStatus is what the operator observed. Only the operator
// writes it.
type QuorumClusterStatus struct {
	// Phase is a one-word summary: Pending, Running, Degraded or
	// Unavailable.
	// +optional
	Phase QuorumClusterPhase `json:"phase,omitempty"`

	// Leader is the node ID of the current Raft leader, e.g. "quorum-0".
	// Empty while there is no leader.
	// +optional
	Leader string `json:"leader,omitempty"`

	// Members are the node IDs in the Raft configuration, as reported by
	// the leader.
	// +optional
	Members []string `json:"members,omitempty"`

	// ReadyReplicas is the number of pods passing their readiness probe.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas"`

	// ObservedGeneration is the spec generation this status was computed
	// for. If it's lower than metadata.generation, the operator hasn't
	// caught up with the latest spec change yet.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions are the standard Kubernetes way to report state, so
	// tools like `kubectl wait --for=condition=Available` work.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// QuorumCluster is a Raft-replicated key-value store (quorum-server)
// managed by the quorum operator.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=qc
// The name prefixes pod and Service names ("<name>-headless", "<name>-0"),
// which Kubernetes limits to 63 characters, so keep it short.
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 50",message="name must be at most 50 characters"
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Leader",type=string,JSONPath=`.status.leader`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type QuorumCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QuorumClusterSpec   `json:"spec"`
	Status QuorumClusterStatus `json:"status,omitempty"`
}

// QuorumClusterList is a list of QuorumClusters.
//
// +kubebuilder:object:root=true
type QuorumClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QuorumCluster `json:"items"`
}

// init registers the Go types with the scheme, so the client knows which
// Kind and API group a *QuorumCluster maps to.
func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &QuorumCluster{}, &QuorumClusterList{})
		return nil
	})
}
