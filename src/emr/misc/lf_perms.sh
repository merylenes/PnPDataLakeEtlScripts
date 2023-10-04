#ROLE='AD-Data-Lake-Administrator'
ROLE='20fifty_deployer'
ROLE='bw-ingestion-glue'
ROLE='20fiftyDataPipelineResourceRole'
#TABLE='hamish_forecasting_bw_hsfaw01'
#DB='test_stz_retail_prod'
DB='stz-finance-dev'
TABLE='cos_bw_hfglm01'

# DB permissions
aws lakeformation grant-permissions \
    --catalog-id 538635328987 \
    --principal "{\"DataLakePrincipalIdentifier\":\"arn:aws:iam::538635328987:role/${ROLE}\"}" \
    --resource "{\"Database\":{\"Name\": \"${DB}\"}}" \
    --permissions '["ALL"]' \
    --profile pnp-emr-1

# TABLE permissions
ROLE='AD-Data-Lake-Administrator'
ROLE='AD-Data-Lake-Developers'
ROLE='20fiftyDataPipelineResourceRole'
DB='stz-retail-dev'
TABLE='availability_bw_hravm01'
aws lakeformation grant-permissions \
    --catalog-id 538635328987 \
    --principal "{\"DataLakePrincipalIdentifier\":\"arn:aws:iam::538635328987:role/${ROLE}\"}" \
    --resource "{\"Table\":{\"DatabaseName\": \"${DB}\", \"Name\":\"${TABLE}\"}}" \
    --permissions '["ALL"]' \
    --profile pnp-emr-1

aws lakeformation grant-permissions \
    --catalog-id 538635328987 \
    --principal "{\"DataLakePrincipalIdentifier\":\"arn:aws:iam::538635328987:role/${ROLE}\"}" \
    --resource "{\"Table\":{\"DatabaseName\": \"${DB}\", \"Name\":\"${TABLE}\"}}" \
    --permissions '[ "SELECT", "ALTER", "DELETE", "DESCRIBE", "DROP", "INSERT"]' \
    --permissions-with-grant-option '["SELECT",  "ALTER", "DELETE", "DESCRIBE", "DROP", "INSERT"]' \
    --profile pnp-emr-1

### ===== List Permissions for the tables and databases
ROLE='20fiftyDataPipelineResourceRole'
DB='stz-retail-dev'
TABLE='forecasting_bw_hsfaw01'
aws lakeformation list-permissions \
    --catalog-id 538635328987 \
    --principal "{\"DataLakePrincipalIdentifier\":\"arn:aws:iam::538635328987:role/${ROLE}\"}" \
    --resource-type DATABASE --resource "{\"Database\":{\"Name\":\"${DB}\"}}" \
    --profile pnp-emr-1


# For the table
aws lakeformation list-permissions \
    --principal "{\"DataLakePrincipalIdentifier\":\"arn:aws:iam::538635328987:role/${ROLE}\"}" \
    --resource-type TABLE \
    --resource "{\"Table\":{\"DatabaseName\":\"${DB}\",\"Name\":\"${TABLE}\"}}" \
    --profile pnp-emr-1

