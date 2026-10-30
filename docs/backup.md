# MySQLCluster 备份操作

当前实验集群的 `mysql-lab/mysqlcluster-sample` 已启用每日全量备份。北京时间 02:00，Operator 创建的 CronJob 从 `slave-service` 选择一个健康从库，执行 `mysqldump --single-transaction --all-databases`，压缩后写入独立的 `mysqlcluster-sample-backup` PVC。备份文件采用 `.partial` 临时名称，导出完成后才改成 `.sql.gz`，并生成 SHA256 文件。

备份 PVC 使用 `local-path`，目前绑定在 `k8s-node-01`，申请容量 5 GiB。该存储类把文件保存在单个节点本地；节点或磁盘故障仍可能同时丢失备份。需要跨节点保护时，应将备份 PVC 换成具备副本或远端存储能力的 StorageClass，并定期做恢复演练。

## 手动备份

每次使用新的请求值；Operator 创建 Job 后会自动移除该注解。同一个请求值重复提交不会再创建新的 Job。

```bash
kubectl -n mysql-lab annotate mysqlcluster mysqlcluster-sample \
  apps.egonlin.com/backup-request=manual-$(date +%Y%m%d-%H%M%S) --overwrite
kubectl -n mysql-lab get job -l app=mysql-backup
kubectl -n mysql-lab get mysqlcluster mysqlcluster-sample -o jsonpath='{.status.backup}'
```

查看某次 Job 的输出：

```bash
kubectl -n mysql-lab logs job/<job-name>
```

## 恢复检查

不要直接把全量备份导入正在服务的 MySQL。先创建隔离的 MySQL 实例，挂载备份 PVC 为只读，使用全新空数据目录；确认数据库和表完整后，再决定正式恢复范围。

```bash
cd /backup
sha256sum -c <backup-file>.sql.gz.sha256
gzip -t <backup-file>.sql.gz
gzip -cd <backup-file>.sql.gz | MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot
MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -e 'SHOW DATABASES;'
```

本次已在独立临时 MySQL 5.7 Pod 中验证恢复，`codex_failover_check` 和 `ops_verify` 两个库均可见，临时 Pod 已清理。

## 配置位置

- CR 示例：`config/samples/apps_v1_mysqlcluster.yaml` 中的 `spec.backup`。
- 备份逻辑：`internal/controller/backup.go`。
- CRD 字段：`config/crd/bases/apps.egonlin.com_mysqlclusters.yaml`。
- Job/CronJob 权限：`config/rbac/role.yaml`。

当前 CronJob 保留最近 3 个成功 Job 和 1 个失败 Job 的 Kubernetes 对象；它不会自动删除 PVC 中的备份文件。应结合容量规划增加文件清理或转存策略。这个实验版本沿用 Operator 内现有的 MySQL root 密码，正式环境应改用独立备份账号和外部 Secret。
