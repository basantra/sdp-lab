variable "tenancy_ocid" {
  type        = string
  description = "OCID of the OCI tenancy"
}

variable "compartment_ocid" {
  type        = string
  description = "OCID of the compartment where resources will be provisioned"
}

variable "region" {
  type        = string
  description = "OCI region"
  default     = "us-ashburn-1"
}

variable "oci_profile" {
  type        = string
  description = "Profile name in ~/.oci/config"
  default     = "JDETRIAL"
}

variable "user_ocid" {
  type        = string
  description = "OCID of the OCI user (optional if read from ~/.oci/config profile)"
  default     = null
}

variable "fingerprint" {
  type        = string
  description = "Fingerprint of the OCI API key (optional if read from ~/.oci/config profile)"
  default     = null
}

variable "private_key_path" {
  type        = string
  description = "Path to the OCI API private key (optional if read from ~/.oci/config profile)"
  default     = null
}

variable "ssh_public_key_path" {
  type        = string
  description = "Path to the SSH public key for instance access"
  default     = "~/.ssh/id_ed25519.pub"
}

variable "my_ip" {
  type        = string
  description = "Client public IP in CIDR format (e.g. 203.0.113.10/32) allowed for SSH and Spark UI"
}

variable "instance_name" {
  type        = string
  description = "Display name for the Spark lab compute instance"
  default     = "sdp-spark-lab"
}

variable "instance_shape" {
  type        = string
  description = "Compute shape"
  default     = "VM.Standard.E5.Flex"
}

variable "instance_ocpus" {
  type        = number
  description = "Number of OCPUs"
  default     = 1
}

variable "instance_memory_in_gbs" {
  type        = number
  description = "RAM in GBs"
  default     = 8
}

variable "boot_volume_size_in_gbs" {
  type        = number
  description = "Size of the boot volume in GB (up to 200 GB Always Free across tenancy)"
  default     = 100
}

variable "bucket_name" {
  type        = string
  description = "Name of the Object Storage bucket"
  default     = "sdp-lab-data"
}
