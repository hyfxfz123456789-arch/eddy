package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 1、定制CRD必须要有的字段：CR文件中引用的字段是json后的字段
// 子结构体：用于定制存储
type StorageConfig struct {
	StorageClassName string `json:"storageClassName"`
	Size             string `json:"size"`
}

// BackupConfig configures daily logical backups to a dedicated PVC.
type BackupConfig struct {
	Enabled  bool          `json:"enabled,omitempty"`
	Schedule string        `json:"schedule,omitempty"`
	TimeZone string        `json:"timeZone,omitempty"`
	Storage  StorageConfig `json:"storage,omitempty"`
}

// 子结构体：用于资源限制
// 资源请求定义
type ResourceRequests struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

// 资源限制定义
type ResourceLimits struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

// 资源要求定义
type ResourceRequirements struct {
	Requests ResourceRequests `json:"requests"`
	Limits   ResourceLimits   `json:"limits"`
}

type MysqlClusterSpec struct {
	//MasterConfig  string `json:"masterConfig"`
	//SlaveConfig   string `json:"slaveConfig"`
	Image         string               `json:"image"`
	Replicas      int32                `json:"replicas"`
	MasterService string               `json:"masterService"`
	SlaveService  string               `json:"slaveService"`
	Storage       StorageConfig        `json:"storage"`
	Backup        BackupConfig         `json:"backup,omitempty"`
	Resources     ResourceRequirements `json:"resources"`
}

// 2、定制status信息
type MysqlClusterStatus struct {
	Master string       `json:"master"`
	Slaves []string     `json:"slaves"`
	Backup BackupStatus `json:"backup,omitempty"`
}

type BackupStatus struct {
	LastJob         string `json:"lastJob,omitempty"`
	Phase           string `json:"phase,omitempty"`
	LastSuccessTime string `json:"lastSuccessTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

type MysqlCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MysqlClusterSpec   `json:"spec,omitempty"`
	Status MysqlClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type MysqlClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MysqlCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MysqlCluster{}, &MysqlClusterList{})
}
