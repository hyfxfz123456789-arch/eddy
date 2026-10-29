package controller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	databasev1 "github.com/egonlin/api/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	masterNameAnnotation    = "apps.egonlin.com/current-master"
	masterUUIDAnnotation    = "apps.egonlin.com/master-uuid"
	failoverSinceAnnotation = "apps.egonlin.com/failover-since"
	failoverGrace           = 20 * time.Second
)

// Remember the source UUID before its Pod disappears. Only an actual healthy
// Service target may replace this identity.
func (r *MysqlClusterReconciler) observeMasterIdentity(ctx context.Context, cluster *databasev1.MysqlCluster) (bool, error) {
	if cluster.Annotations[failoverSinceAnnotation] != "" {
		return false, nil
	}
	name, err := r.getMasterPodNameFromEndpoints(ctx, *cluster)
	if err != nil {
		return false, nil // The failure path will inspect the Endpoint separately.
	}
	pod := &v1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
		return false, err
	}
	if !isPodHealthy(*pod) || pod.Labels["role"] != "master" {
		return false, nil
	}
	output, err := r.execCommandOnPod(pod, fmt.Sprintf("mysql -uroot -p%s -N -B -e 'SELECT @@GLOBAL.server_uuid'", MySQLPassword))
	if err != nil {
		return false, err
	}
	uuid := strings.TrimSpace(output)
	if uuid == "" {
		return false, fmt.Errorf("empty server UUID on %s", name)
	}
	if cluster.Annotations[masterNameAnnotation] == name && cluster.Annotations[masterUUIDAnnotation] == uuid {
		return false, nil
	}
	copy := cluster.DeepCopy()
	if copy.Annotations == nil {
		copy.Annotations = map[string]string{}
	}
	copy.Annotations[masterNameAnnotation] = name
	copy.Annotations[masterUUIDAnnotation] = uuid
	if err := r.Update(ctx, copy); err != nil {
		return false, err
	}
	return true, nil
}

// Lab-only automatic promotion. A vanished Endpoint is not a production
// fencing guarantee: a partitioned old master may still accept direct writes.
func (r *MysqlClusterReconciler) handleMasterFailure(ctx context.Context, cluster databasev1.MysqlCluster) (ctrl.Result, error) {
	if os.Getenv("LAB_AUTO_FAILOVER") != "true" {
		return ctrl.Result{}, fmt.Errorf("primary Endpoint absent; LAB_AUTO_FAILOVER is disabled")
	}
	oldName := cluster.Annotations[masterNameAnnotation]
	oldUUID := cluster.Annotations[masterUUIDAnnotation]
	if oldName == "" || oldUUID == "" {
		return ctrl.Result{}, fmt.Errorf("primary Endpoint absent and prior master identity is unknown")
	}
	old := &v1.Pod{}
	err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: oldName}, old)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if err == nil && isPodHealthy(*old) && old.Labels["role"] == "master" {
		if _, probeErr := r.execCommandOnPod(old, fmt.Sprintf("mysql -uroot -p%s -N -B -e 'SELECT 1'", MySQLPassword)); probeErr == nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}
	sinceText := cluster.Annotations[failoverSinceAnnotation]
	if sinceText == "" {
		copy := cluster.DeepCopy()
		copy.Annotations[failoverSinceAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
		if err := r.Update(ctx, copy); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	since, err := time.Parse(time.RFC3339Nano, sinceText)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("invalid failover timestamp: %w", err)
	}
	if remaining := failoverGrace - time.Since(since); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	newName, replicas, err := r.electNewMaster(ctx, cluster, oldUUID)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Remove the failed Pod from the primary Service before promotion.
	if old.Name != "" && old.Labels["role"] == "master" {
		if err := r.labelPod(ctx, oldName, "failed-master", cluster); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.setupMasterSlaveReplication(ctx, newName, replicas, cluster); err != nil {
		return ctrl.Result{}, err
	}
	newPod := &v1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: newName}, newPod); err != nil {
		return ctrl.Result{}, err
	}
	output, err := r.execCommandOnPod(newPod, fmt.Sprintf("mysql -uroot -p%s -N -B -e 'SELECT @@GLOBAL.server_uuid'", MySQLPassword))
	if err != nil {
		return ctrl.Result{}, err
	}
	copy := cluster.DeepCopy()
	copy.Annotations[masterNameAnnotation] = newName
	copy.Annotations[masterUUIDAnnotation] = strings.TrimSpace(output)
	delete(copy.Annotations, failoverSinceAnnotation)
	if err := r.Update(ctx, copy); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}
