dt=$(date +"%Y%m%d")
STAGE='dev'

# This step is simply to change the concurrency of steps

aws emr modify-cluster --cluster-id $(jq -r '.jobFlowId' /emr/instance-controller/lib/info/job-flow.json) --step-concurrency-level $1

exit 0