resource "oci_core_vcn" "sdp_vcn" {
  compartment_id = var.compartment_ocid
  cidr_blocks    = ["10.0.0.0/16"]
  display_name   = "sdp-lab-vcn"
  dns_label      = "sdplabvcn"
}

resource "oci_core_internet_gateway" "sdp_ig" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.sdp_vcn.id
  display_name   = "sdp-lab-igw"
  enabled        = true
}

resource "oci_core_route_table" "sdp_public_rt" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.sdp_vcn.id
  display_name   = "sdp-lab-public-rt"

  route_rules {
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    network_entity_id = oci_core_internet_gateway.sdp_ig.id
  }
}

resource "oci_core_security_list" "sdp_public_sl" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.sdp_vcn.id
  display_name   = "sdp-lab-public-sl"

  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
    stateless   = false
  }

  ingress_security_rules {
    protocol    = "6" # TCP
    source      = var.my_ip
    source_type = "CIDR_BLOCK"
    stateless   = false
    description = "SSH access from client IP only"

    tcp_options {
      min = 22
      max = 22
    }
  }

  ingress_security_rules {
    protocol    = "6" # TCP
    source      = var.my_ip
    source_type = "CIDR_BLOCK"
    stateless   = false
    description = "Spark UI access from client IP only"

    tcp_options {
      min = 4040
      max = 4040
    }
  }
}

resource "oci_core_subnet" "sdp_public_subnet" {
  compartment_id    = var.compartment_ocid
  vcn_id            = oci_core_vcn.sdp_vcn.id
  cidr_block        = "10.0.1.0/24"
  display_name      = "sdp-lab-public-subnet"
  dns_label         = "sdppublic"
  route_table_id    = oci_core_route_table.sdp_public_rt.id
  security_list_ids = [oci_core_security_list.sdp_public_sl.id]
}
