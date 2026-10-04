#!/usr/bin/env bash
set -uo pipefail

if [ -f /etc/profile.d/spark.sh ]; then
    source /etc/profile.d/spark.sh
fi
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export PATH="${SPARK_HOME}/bin:${PATH}"

cd /home/ubuntu/sdp-lab/pipelines/streaming

echo "=== Launching spark-pipelines run ==="
spark-pipelines run > /home/ubuntu/sdp-lab/pipelines/streaming/kill_test.log 2>&1 &
CLI_PID=$!
echo "CLI_PID=${CLI_PID}"

echo "Waiting for rate_stream to reach RUNNING state..."
RUNNING=0
for i in $(seq 1 300); do
    if grep -q "Flow spark_catalog.default.rate_stream is RUNNING" /home/ubuntu/sdp-lab/pipelines/streaming/kill_test.log 2>/dev/null; then
        echo "Detected: rate_stream is RUNNING!"
        RUNNING=1
        break
    fi
    sleep 0.1
done

if [ "${RUNNING}" -eq 0 ]; then
    echo "rate_stream did not reach RUNNING in time!"
    cat /home/ubuntu/sdp-lab/pipelines/streaming/kill_test.log
    exit 1
fi

JAVA_PID=$(pgrep -f org.apache.spark.deploy.SparkPipelines | head -n 1 || true)
echo "JAVA_PID=${JAVA_PID}"

echo "=== Sending SIGKILL (kill -9) to SparkPipelines and CLI ==="
if [ -n "${JAVA_PID}" ]; then
    echo "Killing Java PID ${JAVA_PID} with SIGKILL..."
    kill -9 "${JAVA_PID}"
fi
echo "Killing CLI PID ${CLI_PID} with SIGKILL..."
kill -9 "${CLI_PID}" 2>/dev/null || true

wait "${CLI_PID}" 2>/dev/null || true
EXIT_CODE=$?
echo "CLI Process exited with wait code: ${EXIT_CODE}"
