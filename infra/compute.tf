# ==============================================================================
# Operating System Selection Justification: Canonical Ubuntu 22.04 LTS (ARM64)
# ==============================================================================
# We select Canonical Ubuntu 22.04 LTS (aarch64) over Oracle Linux 9 for this
# Spark lab for three concrete reasons:
#
# 1. Spark & PySpark Ecosystem Compatibility:
#    The broader data engineering and Python data science ecosystem (PySpark,
#    Delta Lake, PyArrow, NumPy, DuckDB) heavily targets Ubuntu/Debian LTS in CI/CD
#    and pre-compiled aarch64 manylinux wheels. Ubuntu 22.04 provides the smoothest
#    path for installing Python virtual environments, pip dependencies, and OpenJDK
#    (openjdk-17-jdk) without encountering missing native shared libraries or
#    yum/dnf repo fragmentation.
#
# 2. Host Firewall Behavior (Spark UI Port 4040):
#    Oracle Linux 9 enables `firewalld` with strict default zones out of the box,
#    which silently drops incoming traffic to non-standard ports (such as 4040 for
#    the Spark Web UI) on the host even when OCI VCN Security Lists explicitly allow
#    them. Ubuntu 22.04 does not enable restrictive host firewall rules by default,
#    allowing the OCI Security List to act as the single source of truth for
#    network ingress security.
#
# 3. Community Adoption & Upstream Spark Conventions:
#    Most Spark and PySpark deployment guides, tutorials, and troubleshooting docs
#    assume Ubuntu/Debian file system paths, package managers (apt), and systemd
#    conventions, significantly reducing friction during lab experimentation.
# ==============================================================================

data "oci_identity_availability_domains" "ads" {
  compartment_id = var.compartment_ocid
}

data "oci_core_images" "ubuntu" {
  compartment_id           = var.compartment_ocid
  operating_system         = "Canonical Ubuntu"
  operating_system_version = "22.04"
  shape                    = var.instance_shape
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"
}

resource "oci_core_instance" "spark_lab" {
  availability_domain = data.oci_identity_availability_domains.ads.availability_domains[0].name
  compartment_id      = var.compartment_ocid
  display_name        = var.instance_name
  shape               = var.instance_shape

  shape_config {
    ocpus         = var.instance_ocpus
    memory_in_gbs = var.instance_memory_in_gbs
  }

  source_details {
    source_type             = "image"
    source_id               = data.oci_core_images.ubuntu.images[0].id
    boot_volume_size_in_gbs = var.boot_volume_size_in_gbs
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.sdp_public_subnet.id
    assign_public_ip = true
    display_name     = "${var.instance_name}-vnic"
  }

  metadata = {
    ssh_authorized_keys = file(pathexpand(var.ssh_public_key_path))
  }
}
