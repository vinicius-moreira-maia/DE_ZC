variable "credential" {
  description = "credencial da conta de serviço"
  default     = "/home/vmm/DE_ZC/01-DockerPostgresGCPTerraform/terrademo/keys/my-cred.json"
}

variable "project" {
  description = "Project"
  default     = "taxi-rides-ny-448023"
}

variable "region" {
  description = "Região"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "Nome do Dataset do BigQuery"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Nome do Bucket"
  default     = "taxi-rides-ny-448023-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDART"
}