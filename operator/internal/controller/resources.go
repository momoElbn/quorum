package controller

// This file builds the desired state: the four Kubernetes objects a
// QuorumCluster needs. They are the same objects as charts/quorum, built in
// Go instead of templates.
//
// Each function returns an "apply configuration": a description of only the
// fields we care about. The reconciler sends it with server-side apply, and
// the API server merges it into the live object. If nothing differs, the API
// server changes nothing. That is what makes reconciling idempotent: running
// it 1 or 100 times gives the same result.

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	policyv1ac "k8s.io/client-go/applyconfigurations/policy/v1"

	quorumv1alpha1 "github.com/momoElbn/quorum/operator/api/v1alpha1"
)

const (
	httpPort = 8080 // client API, /status, /healthz, /metrics
	raftPort = 7000 // Raft traffic between pods
	dataDir  = "/data"

	// nonRootUID is the "nonroot" user of the distroless base image.
	nonRootUID = 65532
)

// headlessName is the headless Service that gives each pod a DNS name.
func headlessName(qc *quorumv1alpha1.QuorumCluster) string {
	return qc.Name + "-headless"
}

// podName is the name of pod i. A StatefulSet always names its pods
// <statefulset-name>-<ordinal>, so we know the names in advance.
func podName(qc *quorumv1alpha1.QuorumCluster, i int32) string {
	return fmt.Sprintf("%s-%d", qc.Name, i)
}

// podHostByName is the stable DNS name of the pod called name.
func podHostByName(qc *quorumv1alpha1.QuorumCluster, name, clusterDomain string) string {
	return fmt.Sprintf("%s.%s.%s.svc.%s", name, headlessName(qc), qc.Namespace, clusterDomain)
}

// podHost is pod i's stable DNS name, e.g.
// quorum-0.quorum-headless.quorum.svc.cluster.local.
func podHost(qc *quorumv1alpha1.QuorumCluster, i int32, clusterDomain string) string {
	return podHostByName(qc, podName(qc, i), clusterDomain)
}

// selectorLabels find this cluster's pods. They must never change: a
// StatefulSet's selector is immutable.
func selectorLabels(qc *quorumv1alpha1.QuorumCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "quorum",
		"app.kubernetes.io/instance": qc.Name,
	}
}

// labels go on every object we create.
func labels(qc *quorumv1alpha1.QuorumCluster) map[string]string {
	l := selectorLabels(qc)
	l["app.kubernetes.io/managed-by"] = "quorum-operator"
	return l
}

// ownerRef marks an object as owned by the QuorumCluster. It does two
// things:
//   - Garbage collection: deleting the QuorumCluster makes Kubernetes delete
//     everything it owns. The operator needs no cleanup code.
//   - Watching: changes to owned objects (e.g. a pod becoming ready changes
//     the StatefulSet status) trigger a reconcile of the owner.
//
// Controller=true means "this owner manages the object". An object can have
// only one controller.
func ownerRef(qc *quorumv1alpha1.QuorumCluster) *metav1ac.OwnerReferenceApplyConfiguration {
	return metav1ac.OwnerReference().
		WithAPIVersion(quorumv1alpha1.GroupVersion.String()).
		WithKind("QuorumCluster").
		WithName(qc.Name).
		WithUID(qc.UID).
		WithController(true).
		WithBlockOwnerDeletion(true)
}

// headlessService gives every pod a stable DNS name. Raft stores these names
// in its membership list, because pod IPs change on every restart.
func headlessService(qc *quorumv1alpha1.QuorumCluster) *corev1ac.ServiceApplyConfiguration {
	return corev1ac.Service(headlessName(qc), qc.Namespace).
		WithLabels(labels(qc)).
		// Both Services select the same pods, so they need a label that tells
		// them apart: the quorum ServiceMonitor scrapes through this one only.
		// (Scraping through both would collect every metric twice.) This one
		// also lists pods that aren't ready, so they're still scraped.
		WithLabels(map[string]string{"app.kubernetes.io/component": "peers"}).
		WithOwnerReferences(ownerRef(qc)).
		WithSpec(corev1ac.ServiceSpec().
			WithClusterIP(corev1.ClusterIPNone).
			// Publish DNS records for pods that aren't Ready yet. This is
			// essential: a pod is Ready (/readyz) only once it knows a
			// leader, and a leader can only be elected if the pods can
			// reach each other by name. Without this, unready pods would
			// lose their DNS names, so they couldn't elect a leader, so
			// they would never become ready (on a cold start or after a
			// majority restarts).
			WithPublishNotReadyAddresses(true).
			WithSelector(selectorLabels(qc)).
			WithPorts(
				servicePort("http", httpPort),
				servicePort("raft", raftPort),
			))
}

// clientService is the one stable name clients use. It spreads requests over
// the Ready pods, and followers redirect writes to the leader.
func clientService(qc *quorumv1alpha1.QuorumCluster) *corev1ac.ServiceApplyConfiguration {
	return corev1ac.Service(qc.Name, qc.Namespace).
		WithLabels(labels(qc)).
		WithLabels(map[string]string{"app.kubernetes.io/component": "client"}).
		WithOwnerReferences(ownerRef(qc)).
		WithSpec(corev1ac.ServiceSpec().
			WithType(corev1.ServiceTypeClusterIP).
			WithSelector(selectorLabels(qc)).
			WithPorts(servicePort("http", httpPort)))
}

func servicePort(name string, port int32) *corev1ac.ServicePortApplyConfiguration {
	return corev1ac.ServicePort().
		WithName(name).
		WithProtocol(corev1.ProtocolTCP).
		WithPort(port).
		WithTargetPort(intstr.FromString(name))
}

// podDisruptionBudget stops planned evictions (node drains, upgrades) from
// taking down more than one pod at a time. Raft needs a majority.
func podDisruptionBudget(qc *quorumv1alpha1.QuorumCluster) *policyv1ac.PodDisruptionBudgetApplyConfiguration {
	return policyv1ac.PodDisruptionBudget(qc.Name, qc.Namespace).
		WithLabels(labels(qc)).
		WithOwnerReferences(ownerRef(qc)).
		WithSpec(policyv1ac.PodDisruptionBudgetSpec().
			WithMaxUnavailable(intstr.FromInt32(1)).
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(selectorLabels(qc))))
}

// statefulSet runs the Raft nodes. A StatefulSet gives each pod a stable name
// (its Raft ID), a stable DNS name (through the headless Service) and its own
// volume that follows it across restarts.
//
// replicas is how many pods to run. In safe scaling mode the operator moves
// it one at a time towards spec.replicas; see planScale.
//
// Nothing in the pod template may depend on the replica count. Changing the
// template restarts every pod (a rolling update), and a scale operation
// must not do that: it would cost a leader election per pod in the middle
// of a membership change. That's why there is no PEERS_HTTP (it would list
// every pod): the server works out the leader's HTTP address from its Raft
// address instead.
func statefulSet(qc *quorumv1alpha1.QuorumCluster, clusterDomain string, replicas int32) *appsv1ac.StatefulSetApplyConfiguration {
	envs := []*corev1ac.EnvVarApplyConfiguration{
		// The downward API hands the pod its own name and namespace.
		fieldEnv("POD_NAME", "metadata.name"),
		fieldEnv("POD_NAMESPACE", "metadata.namespace"),
		// The pod name (quorum-0, ...) is stable, so it is the Raft ID.
		env("NODE_ID", "$(POD_NAME)"),
		// The stable DNS name other pods dial. Raft stores it in the
		// membership, so it must not be the pod IP.
		env("RAFT_ADDR", fmt.Sprintf("$(POD_NAME).%s.$(POD_NAMESPACE).svc.%s:%d", headlessName(qc), clusterDomain, raftPort)),
		env("HTTP_ADDR", fmt.Sprintf(":%d", httpPort)),
		env("DATA_DIR", dataDir),
		// Only pod 0 creates the cluster, and only if its volume is empty.
		env("BOOTSTRAP_NODE", podName(qc, 0)),
	}
	if qc.Spec.UnsafeScaling {
		// The naive way: every pod joins by itself, through pod 0, which
		// redirects the join to the leader. In safe mode there's no
		// JOIN_ADDR: the operator adds each pod, as a nonvoter first.
		envs = append(envs, env("JOIN_ADDR", fmt.Sprintf("%s:%d", podHost(qc, 0, clusterDomain), httpPort)))
	}

	// When the StatefulSet shrinks, delete the removed pod's volume. In
	// safe mode the member was already removed from Raft; if its old log
	// survived, scaling up again would bring back a node that still
	// believes in an old configuration. A fresh volume makes it join as a
	// brand-new node. Unsafe mode keeps Kubernetes' default (Retain), as
	// the naive baseline. Deleting the QuorumCluster keeps the volumes
	// either way: losing Raft data must be deliberate.
	whenScaled := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	if !qc.Spec.UnsafeScaling {
		whenScaled = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	}

	container := corev1ac.Container().
		WithName("quorum-server").
		WithImage(qc.Spec.Image).
		// IfNotPresent: images are loaded into k3d with `k3d image import`,
		// not pulled from a registry.
		WithImagePullPolicy(corev1.PullIfNotPresent).
		WithPorts(
			corev1ac.ContainerPort().WithName("http").WithContainerPort(httpPort).WithProtocol(corev1.ProtocolTCP),
			corev1ac.ContainerPort().WithName("raft").WithContainerPort(raftPort).WithProtocol(corev1.ProtocolTCP),
		).
		WithEnv(envs...).
		WithVolumeMounts(corev1ac.VolumeMount().WithName("data").WithMountPath(dataDir)).
		// Liveness: /healthz doesn't check for a leader. Failing liveness
		// during a network split would restart every pod and slow down
		// recovery.
		WithLivenessProbe(httpProbe("/healthz", 10, 3)).
		// Readiness: /readyz fails while the pod can't serve clients (no
		// leader, cut off from it, not a voter, far behind). Kubernetes then
		// only takes the pod out of the client Service. Checked every
		// second, so a pod leaves the Service within about 3s of losing
		// the leader and rejoins 1s after it recovers.
		WithReadinessProbe(httpProbe("/readyz", 1, 3).WithSuccessThreshold(1)).
		WithResources(corev1ac.ResourceRequirements().
			WithRequests(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			}).
			WithLimits(corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			})).
		WithSecurityContext(corev1ac.SecurityContext().
			WithAllowPrivilegeEscalation(false).
			WithReadOnlyRootFilesystem(true). // all writes go to /data
			WithCapabilities(corev1ac.Capabilities().WithDrop("ALL")))

	podSpec := corev1ac.PodSpec().
		// Room for the server's shutdown: drain HTTP, hand off leadership,
		// close Raft.
		WithTerminationGracePeriodSeconds(30).
		WithSecurityContext(corev1ac.PodSecurityContext().
			WithRunAsNonRoot(true).
			WithRunAsUser(nonRootUID).
			WithRunAsGroup(nonRootUID).
			WithFSGroup(nonRootUID). // makes the volume writable for that user
			WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault))).
		// Prefer one pod per node: losing a node then costs only one Raft
		// member. "Preferred" so it still schedules on small clusters.
		WithAffinity(corev1ac.Affinity().
			WithPodAntiAffinity(corev1ac.PodAntiAffinity().
				WithPreferredDuringSchedulingIgnoredDuringExecution(corev1ac.WeightedPodAffinityTerm().
					WithWeight(100).
					WithPodAffinityTerm(corev1ac.PodAffinityTerm().
						WithTopologyKey("kubernetes.io/hostname").
						WithLabelSelector(metav1ac.LabelSelector().WithMatchLabels(selectorLabels(qc))))))).
		WithContainers(container)

	// One PersistentVolumeClaim per pod (data-<name>-0, ...), bound to that
	// pod for good. It holds the Raft log, vote and snapshots. Built as a
	// bare struct (no apiVersion/kind/namespace), because the template is
	// immutable and must look the same on every apply.
	claim := &corev1ac.PersistentVolumeClaimApplyConfiguration{}
	claim.WithName("data").
		WithLabels(selectorLabels(qc)).
		WithSpec(corev1ac.PersistentVolumeClaimSpec().
			WithAccessModes(corev1.ReadWriteOnce).
			WithResources(corev1ac.VolumeResourceRequirements().
				WithRequests(corev1.ResourceList{corev1.ResourceStorage: qc.Spec.StorageSize})))

	return appsv1ac.StatefulSet(qc.Name, qc.Namespace).
		WithLabels(labels(qc)).
		WithOwnerReferences(ownerRef(qc)).
		WithSpec(appsv1ac.StatefulSetSpec().
			WithReplicas(replicas).
			WithServiceName(headlessName(qc)).
			WithPersistentVolumeClaimRetentionPolicy(appsv1ac.StatefulSetPersistentVolumeClaimRetentionPolicy().
				WithWhenScaled(whenScaled).
				WithWhenDeleted(appsv1.RetainPersistentVolumeClaimRetentionPolicyType)).
			// Start all pods at once: Raft nodes need each other to elect a
			// leader, so starting them one by one only slows things down.
			// Rolling updates still replace one pod at a time.
			WithPodManagementPolicy(appsv1.ParallelPodManagement).
			WithUpdateStrategy(appsv1ac.StatefulSetUpdateStrategy().WithType(appsv1.RollingUpdateStatefulSetStrategyType)).
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(selectorLabels(qc))).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(labels(qc)).
				WithSpec(podSpec)).
			WithVolumeClaimTemplates(claim))
}

func env(name, value string) *corev1ac.EnvVarApplyConfiguration {
	return corev1ac.EnvVar().WithName(name).WithValue(value)
}

func fieldEnv(name, fieldPath string) *corev1ac.EnvVarApplyConfiguration {
	return corev1ac.EnvVar().WithName(name).WithValueFrom(corev1ac.EnvVarSource().
		WithFieldRef(corev1ac.ObjectFieldSelector().WithFieldPath(fieldPath)))
}

func httpProbe(path string, periodSeconds, failureThreshold int32) *corev1ac.ProbeApplyConfiguration {
	return corev1ac.Probe().
		WithHTTPGet(corev1ac.HTTPGetAction().WithPath(path).WithPort(intstr.FromString("http"))).
		WithPeriodSeconds(periodSeconds).
		WithFailureThreshold(failureThreshold)
}
