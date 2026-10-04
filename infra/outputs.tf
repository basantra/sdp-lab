output "public_ip" {
  description = "Public IPv4 address of the Spark lab compute instance"
  value       = oci_core_instance.spark_lab.public_ip
}

output "bucket_namespace" {
  description = "OCI Object Storage namespace"
  value       = data.oci_objectstorage_namespace.ns.namespace
}

output "bucket_name" {
  description = "Name of the provisioned Object Storage bucket"
  value       = oci_objectstorage_bucket.sdp_bucket.name
}
