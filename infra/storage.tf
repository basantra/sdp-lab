data "oci_objectstorage_namespace" "ns" {
}

resource "oci_objectstorage_bucket" "sdp_bucket" {
  compartment_id = var.compartment_ocid
  name           = var.bucket_name
  namespace      = data.oci_objectstorage_namespace.ns.namespace
  access_type    = "NoPublicAccess"
  storage_tier   = "Standard"
}
