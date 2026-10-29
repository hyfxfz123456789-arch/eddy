package controller

import (
	"context"
	"fmt"
	"time"

	databasev1 "github.com/egonlin/api/v1"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	switchoverTo   = "apps.egonlin.com/switchover-to"
	switchoverFrom = "apps.egonlin.com/switchover-from"
)

// plannedSwitchover handles a requested, reachable-master switchover.
// The from annotation survives controller restarts during a partial switch.
func (r *MysqlClusterReconciler) plannedSwitchover(ctx context.Context, cluster *databasev1.MysqlCluster) error {
	targetName := cluster.Annotations[switchoverTo]
	oldName := cluster.Annotations[switchoverFrom]
	if oldName == "" {
		var err error
		oldName, err = r.getMasterPodNameFromEndpoints(ctx, *cluster)
		if err != nil {
			return fmt.Errorf("planned switchover requires a reachable master: %w", err)
		}
		if oldName == targetName {
			return fmt.Errorf("target %s is already the master", targetName)
		}
		cluster.Annotations[switchoverFrom] = oldName
		return r.Update(ctx, cluster)
	}
	if oldName == targetName {
		return fmt.Errorf("switchover source and target are both %s", oldName)
	}

	old := &v1.Pod{}
	target := &v1.Pod{}
	for _, item := range []struct {
		name string
		pod  *v1.Pod
	}{{oldName, old}, {targetName, target}} {
		if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: item.name}, item.pod); err != nil {
			return fmt.Errorf("get switchover pod %s: %w", item.name, err)
		}
		if !isPodHealthy(*item.pod) {
			return fmt.Errorf("switchover pod %s is not healthy", item.name)
		}
	}
	if target.Labels["role"] != "slave" && target.Labels["role"] != "master" {
		return fmt.Errorf("target %s is not a replica", targetName)
	}

	// If a previous attempt already moved the Service, finish reconfiguration.
	if target.Labels["role"] != "master" {
		if err := r.ensureReadOnly(old, true); err != nil {
			return err
		}
		oldGTID, err := r.getExecutedGTIDSet(old)
		if err != nil {
			return fmt.Errorf("read frozen master GTIDs: %w", err)
		}
		if oldGTID == "" {
			return fmt.Errorf("frozen master %s has an empty GTID set", oldName)
		}
		deadline := time.NewTimer(60 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			targetGTID, err := r.getExecutedGTIDSet(target)
			if err != nil {
				return fmt.Errorf("read target GTIDs: %w", err)
			}
			caughtUp, err := r.gtidContains(target, oldGTID, targetGTID)
			if err != nil {
				return fmt.Errorf("compare switchover GTIDs: %w", err)
			}
			if caughtUp {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline.C:
				return fmt.Errorf("target %s did not catch up to frozen master %s", targetName, oldName)
			case <-ticker.C:
			}
		}
		// The old master leaves the write Service before the new one enters it.
		if err := r.labelPod(ctx, oldName, "demoted", *cluster); err != nil {
			return err
		}
		if err := r.ensureReadOnly(target, false); err != nil {
			return err
		}
		if err := r.labelPod(ctx, targetName, "master", *cluster); err != nil {
			return err
		}
	}

	var pods v1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{"app": "mysql"}); err != nil {
		return err
	}
	var remaining []string
	for _, pod := range pods.Items {
		if pod.Name != targetName {
			if !isPodHealthy(pod) {
				return fmt.Errorf("replica %s is not healthy; switchover remains incomplete", pod.Name)
			}
			remaining = append(remaining, pod.Name)
		}
	}
	if err := r.setupMasterSlaveReplication(ctx, targetName, remaining, *cluster); err != nil {
		return err
	}
	delete(cluster.Annotations, switchoverTo)
	delete(cluster.Annotations, switchoverFrom)
	return r.Update(ctx, cluster)
}
