#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" && pwd)"
log_dir="${script_dir}/logs"

source "${script_dir}/nodes.sh"

echo "Installing..."
go install ../hdfs/server/controller.go   || exit 1 # Exit if compile+install fails
go install ../hdfs/storage/storage_node.go || exit 1 # Exit if compile+install fails
echo "Done!"

echo "Creating log directory: ${log_dir}"
mkdir -pv "${log_dir}"

echo "Starting Controller..."
ssh "${controller}" "${HOME}/go/bin/controller -port 23000" &> "${log_dir}/controller.log" &

echo "${controller}"

echo "Starting Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" "${HOME}/go/bin/storage_node /bigdata/${me}/storage -port 23001 ${controller}:23000" &> "${log_dir}/${node}.log" &
done

echo "Startup complete!"
