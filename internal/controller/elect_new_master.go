package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	databasev1 "github.com/egonlin/api/v1"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Compare transactions originating on the failed master. Replica-local GTIDs
// must not prevent an otherwise equal pair of replicas from being ranked.
func (r *MysqlClusterReconciler) electNewMaster(ctx context.Context, cluster databasev1.MysqlCluster, sourceUUID string) (string, []string, error) {
	if sourceUUID == "" {
		return "", nil, fmt.Errorf("failed master's server UUID is unknown")
	}
	var pods v1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{"app": "mysql", "role": "slave"}); err != nil {
		return "", nil, fmt.Errorf("list replicas: %w", err)
	}
	type candidate struct {
		pod  v1.Pod
		gtid string
	}
	var candidates []candidate
	for _, pod := range pods.Items {
		if !isPodHealthy(pod) {
			continue
		}
		allGTIDs, err := r.getExecutedGTIDSet(&pod)
		if err != nil {
			return "", nil, fmt.Errorf("read executed GTIDs from %s: %w", pod.Name, err)
		}
		candidates = append(candidates, candidate{pod: pod, gtid: originGTIDs(allGTIDs, sourceUUID)})
	}
	if len(candidates) == 0 {
		return "", nil, fmt.Errorf("no healthy replica available for promotion")
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].pod.Name < candidates[j].pod.Name })
	for _, proposed := range candidates {
		superset := true
		for _, other := range candidates {
			if proposed.pod.Name == other.pod.Name {
				continue
			}
			contains, err := r.gtidContains(&proposed.pod, other.gtid, proposed.gtid)
			if err != nil {
				return "", nil, fmt.Errorf("compare GTIDs on %s: %w", proposed.pod.Name, err)
			}
			if !contains {
				superset = false
				break
			}
		}
		if superset {
			var remaining []string
			for _, other := range candidates {
				if other.pod.Name != proposed.pod.Name {
					remaining = append(remaining, other.pod.Name)
				}
			}
			return proposed.pod.Name, remaining, nil
		}
	}
	return "", nil, fmt.Errorf("replicas have incomparable executed GTIDs from former master %s", sourceUUID)
}

func originGTIDs(set, sourceUUID string) string {
	for _, part := range strings.Split(set, ",") {
		part = strings.TrimSpace(part)
		uuid, _, ok := strings.Cut(part, ":")
		if ok && strings.EqualFold(uuid, sourceUUID) {
			return part
		}
	}
	return ""
}

func (r *MysqlClusterReconciler) getExecutedGTIDSet(pod *v1.Pod) (string, error) {
	// --raw avoids mysql batch mode escaping multi-line GTID sets as literal \\n.
	output, err := r.execCommandOnPod(pod, fmt.Sprintf("mysql -uroot -p%s -N -B --raw -e 'SELECT @@GLOBAL.gtid_executed'", MySQLPassword))
	if err != nil {
		return "", err
	}
	return strings.Join(strings.Fields(output), ""), nil
}

func (r *MysqlClusterReconciler) gtidContains(pod *v1.Pod, subset, superset string) (bool, error) {
	for _, set := range []string{subset, superset} {
		for _, ch := range set {
			if !strings.ContainsRune("0123456789abcdefABCDEF:-, \n", ch) {
				return false, fmt.Errorf("invalid character in GTID set")
			}
		}
	}
	query := fmt.Sprintf("SELECT GTID_SUBSET('%s','%s')", subset, superset)
	output, err := r.execCommandOnPod(pod, fmt.Sprintf("mysql -uroot -p%s -N -B -e \"%s\"", MySQLPassword, query))
	if err != nil {
		return false, err
	}
	switch strings.TrimSpace(output) {
	case "1":
		return true, nil
	case "0":
		return false, nil
	default:
		return false, fmt.Errorf("unexpected GTID_SUBSET result %q", strings.TrimSpace(output))
	}
}
