package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"

	databasev1 "github.com/egonlin/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const backupRequestAnnotation = "apps.egonlin.com/backup-request"

// A backup Job performs one full logical dump. Atomic rename prevents an
// interrupted dump from appearing as a completed backup.
const backupScript = `set -euo pipefail
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
file="/backup/${CLUSTER_NAME}-${stamp}-${BACKUP_ID}.sql.gz"
tmp="${file}.partial"
trap 'rm -f "$tmp"' EXIT
MYSQL_PWD="$MYSQL_PASSWORD" mysqldump -h "$MYSQL_HOST" -uroot \
  --single-transaction --quick --routines --events --triggers \
  --set-gtid-purged=OFF --all-databases | gzip -c > "$tmp"
test -s "$tmp"
gzip -t "$tmp"
mv "$tmp" "$file"
trap - EXIT
sha256sum "$file" > "${file}.sha256"
echo "Backup complete: $file"
`

func (r *MysqlClusterReconciler) backupJobTemplate(cluster *databasev1.MysqlCluster, id string) batchv1.JobSpec {
	backoff := int32(2)
	deadline := int64(3600)
	return batchv1.JobSpec{
		BackoffLimit:          &backoff,
		ActiveDeadlineSeconds: &deadline,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mysql-backup", "mysqlcluster": cluster.Name}},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name: "backup", Image: cluster.Spec.Image,
					Command: []string{"bash", "-c", backupScript},
					Env: []corev1.EnvVar{
						{Name: "CLUSTER_NAME", Value: cluster.Name},
						{Name: "BACKUP_ID", Value: id},
						{Name: "MYSQL_HOST", Value: cluster.Spec.SlaveService},
						{Name: "MYSQL_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + "-backup-auth"}, Key: "password"}}},
					},
					VolumeMounts: []corev1.VolumeMount{{Name: "backup", MountPath: "/backup"}},
				}},
				Volumes: []corev1.Volume{{Name: "backup", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cluster.Name + "-backup"}}}},
			},
		},
	}
}

func (r *MysqlClusterReconciler) reconcileBackup(ctx context.Context, cluster *databasev1.MysqlCluster) error {
	if !cluster.Spec.Backup.Enabled {
		cron := &batchv1.CronJob{}
		key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-backup"}
		if err := r.Get(ctx, key, cron); err == nil {
			return r.Delete(ctx, cron)
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	backup := cluster.Spec.Backup
	if backup.Schedule == "" || backup.Storage.StorageClassName == "" || backup.Storage.Size == "" {
		return fmt.Errorf("enabled backup requires schedule, storageClassName and size")
	}
	if _, err := resource.ParseQuantity(backup.Storage.Size); err != nil {
		return fmt.Errorf("invalid backup storage size: %w", err)
	}
	// This lab uses the existing MySQL root password. The Job receives it from
	// a Secret rather than embedding it in the Pod command line.
	auth := &corev1.Secret{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-backup-auth"}
	if err := r.Get(ctx, key, auth); apierrors.IsNotFound(err) {
		auth = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, StringData: map[string]string{"password": MySQLPassword}}
		if err := controllerutil.SetControllerReference(cluster, auth, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, auth); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if err := r.createPVC(ctx, cluster.Name+"-backup", backup.Storage.StorageClassName, cluster.Namespace, backup.Storage.Size, cluster); err != nil {
		return err
	}

	cron := &batchv1.CronJob{}
	key.Name = cluster.Name + "-backup"
	err := r.Get(ctx, key, cron)
	if apierrors.IsNotFound(err) {
		cron = &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}
	} else if err != nil {
		return err
	}
	previousSpec := cron.Spec.DeepCopy()
	cron.Spec.Schedule = backup.Schedule
	if backup.TimeZone != "" {
		cron.Spec.TimeZone = &backup.TimeZone
	}
	cron.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
	success, failed := int32(3), int32(1)
	cron.Spec.SuccessfulJobsHistoryLimit = &success
	cron.Spec.FailedJobsHistoryLimit = &failed
	cron.Spec.JobTemplate.Spec = r.backupJobTemplate(cluster, "scheduled")
	cron.Spec.JobTemplate.ObjectMeta.Labels = map[string]string{"app": "mysql-backup", "mysqlcluster": cluster.Name}
	if err := controllerutil.SetControllerReference(cluster, cron, r.Scheme); err != nil {
		return err
	}
	if cron.ResourceVersion == "" {
		if err := r.Create(ctx, cron); err != nil {
			return err
		}
	} else if !reflect.DeepEqual(*previousSpec, cron.Spec) {
		if err := r.Update(ctx, cron); err != nil {
			return err
		}
	}

	request := cluster.Annotations[backupRequestAnnotation]
	if request == "" {
		return r.updateBackupStatus(ctx, cluster)
	}
	if len(request) > 256 {
		return fmt.Errorf("backup request is too long")
	}
	hash := sha256.Sum256([]byte(request))
	id := hex.EncodeToString(hash[:])[:12]
	job := &batchv1.Job{}
	key.Name = cluster.Name + "-manual-" + id
	err = r.Get(ctx, key, job)
	if apierrors.IsNotFound(err) {
		job = &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: map[string]string{"app": "mysql-backup", "mysqlcluster": cluster.Name}}, Spec: r.backupJobTemplate(cluster, id)}
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, job); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	copy := cluster.DeepCopy()
	delete(copy.Annotations, backupRequestAnnotation)
	if err := r.Update(ctx, copy); err != nil {
		return err
	}
	return r.updateBackupStatus(ctx, cluster)
}

func (r *MysqlClusterReconciler) updateBackupStatus(ctx context.Context, cluster *databasev1.MysqlCluster) error {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(cluster.Namespace), client.MatchingLabels{"app": "mysql-backup", "mysqlcluster": cluster.Name}); err != nil {
		return err
	}
	sort.Slice(jobs.Items, func(i, j int) bool {
		return jobs.Items[i].CreationTimestamp.After(jobs.Items[j].CreationTimestamp.Time)
	})
	status := cluster.Status.Backup
	if len(jobs.Items) > 0 {
		job := jobs.Items[0]
		status.LastJob = job.Name
		status.Phase = "Running"
		if job.Status.Succeeded > 0 {
			status.Phase = "Succeeded"
		}
		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				status.Phase = "Failed"
			}
		}
	}
	for _, job := range jobs.Items {
		if job.Status.Succeeded > 0 && job.Status.CompletionTime != nil {
			status.LastSuccessTime = job.Status.CompletionTime.UTC().Format("2006-01-02T15:04:05Z")
			break
		}
	}
	if reflect.DeepEqual(status, cluster.Status.Backup) {
		return nil
	}
	copy := cluster.DeepCopy()
	copy.Status.Backup = status
	return r.Status().Update(ctx, copy)
}
