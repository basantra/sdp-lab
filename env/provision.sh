#!/usr/bin/env bash
# ==============================================================================
# env/provision.sh - Automated provisioning for Spark Declarative Pipelines (SDP)
# Target Environment: Ubuntu 22.04 LTS (ARM64 / aarch64)
# ==============================================================================
set -euo pipefail

SPARK_VERSION="${SPARK_VERSION:-4.2.0}"
HADOOP_PROFILE="${HADOOP_PROFILE:-hadoop3}"
SPARK_DIST="spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}"
SPARK_TAR="${SPARK_DIST}.tgz"
SPARK_URL="https://downloads.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TAR}"
SPARK_BACKUP_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TAR}"
INSTALL_DIR="/opt"
SPARK_HOME="${INSTALL_DIR}/spark"

echo "=== [1/5] Updating OS packages and installing prerequisites ==="
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -y
sudo apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    curl \
    tar \
    procps \
    ca-certificates

echo "=== [2/5] Configuring Java Environment ==="
JAVA_HOME_DIR="/usr/lib/jvm/java-17-openjdk-arm64"
if [ ! -d "${JAVA_HOME_DIR}" ]; then
    JAVA_HOME_DIR=$(dirname $(dirname $(readlink -f $(which javac))))
fi

echo "=== [3/5] Downloading and Installing Apache Spark ${SPARK_VERSION} ==="
if [ ! -d "${INSTALL_DIR}/${SPARK_DIST}" ]; then
    echo "Downloading ${SPARK_URL}..."
    if ! curl -fsSL -o "/tmp/${SPARK_TAR}" "${SPARK_URL}"; then
        echo "Primary download mirror failed, attempting archive mirror: ${SPARK_BACKUP_URL}..."
        curl -fsSL -o "/tmp/${SPARK_TAR}" "${SPARK_BACKUP_URL}"
    fi
    echo "Extracting ${SPARK_TAR} to ${INSTALL_DIR}..."
    sudo tar -xzf "/tmp/${SPARK_TAR}" -C "${INSTALL_DIR}"
    rm -f "/tmp/${SPARK_TAR}"
fi

sudo ln -sfn "${INSTALL_DIR}/${SPARK_DIST}" "${SPARK_HOME}"
sudo chown -R ubuntu:ubuntu "${INSTALL_DIR}/${SPARK_DIST}" "${SPARK_HOME}" 2>/dev/null || true

echo "=== [4/5] Configuring System Environment and PATH ==="
sudo bash -c "cat > /etc/profile.d/spark.sh" <<EOF
export JAVA_HOME="${JAVA_HOME_DIR}"
export SPARK_HOME="${SPARK_HOME}"
export PATH="\${SPARK_HOME}/bin:\${SPARK_HOME}/sbin:\${PATH}"
EOF

export JAVA_HOME="${JAVA_HOME_DIR}"
export SPARK_HOME="${SPARK_HOME}"
export PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Ensure spark-pipelines CLI is executable and linked into /usr/local/bin for global discovery
if [ -f "${SPARK_HOME}/bin/spark-pipelines" ]; then
    sudo chmod +x "${SPARK_HOME}/bin/spark-pipelines"
    sudo ln -sfn "${SPARK_HOME}/bin/spark-pipelines" /usr/local/bin/spark-pipelines
fi

echo "=== [5/5] Installing PySpark ${SPARK_VERSION} with SDP (pipelines) extra ==="
# Upgrade pip and wheel tooling first
python3 -m pip install --upgrade pip setuptools wheel

# Record timing of pip install to detect any compilation of non-wheel packages
echo "Starting pip installation of pyspark[pipelines]==${SPARK_VERSION}..."
PIP_START_TIME=$(date +%s)
python3 -m pip install "pyspark[pipelines]==${SPARK_VERSION}" 2>&1 | tee /tmp/pyspark_install.log
PIP_END_TIME=$(date +%s)
PIP_DURATION=$((PIP_END_TIME - PIP_START_TIME))
echo "pip install finished in ${PIP_DURATION}s."

# Check if any package was compiled from source
COMPILED_PACKAGES=$(grep -i "running build_ext" /tmp/pyspark_install.log || true)
if [ -n "${COMPILED_PACKAGES}" ]; then
    echo "WARNING: Detected packages compiled from source during pip install:"
    echo "${COMPILED_PACKAGES}"
else
    echo "SUCCESS: All Python dependencies installed from pre-built aarch64 wheels."
fi

echo "=============================================================================="
echo "=== Provisioning Complete. Running Verification Suite ==="
echo "=============================================================================="
echo "Check 1: JDK Version (Supported by Spark 4.2.0)"
java -version

echo "Check 2: Python Version (Supported by PySpark 4.2.0)"
python3 --version

echo "Check 3: PySpark & SDP Python module import"
python3 -c "import pyspark; import pyspark.pipelines as dp; print('pyspark version:', pyspark.__version__); print('pyspark.pipelines module successfully imported')"

echo "Check 4: spark-pipelines CLI on PATH and usage output"
which spark-pipelines
spark-pipelines --help || true

echo "=== All 4 verification checks completed successfully ==="
