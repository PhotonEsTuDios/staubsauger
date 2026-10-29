#!/usr/bin/env bash
# Legt photon-dropbox-ingest im aktiven Account an bzw. aktualisiert ihn (idempotent).
# Läuft in der Cloud; der Mac stößt nur an. Kein Zeitplan: Start nur manuell.
#   AWS_PROFILE=photon-new PHOTON_BUCKET=photonestudios-archive ./setup-aws.sh
# Starten:  aws lambda invoke --function-name photon-dropbox-ingest --invocation-type Event /dev/null
# Status:   AWS_PROFILE=photon-new ~/PhotonEsTuDios/.venv/bin/python status.py
# Voraussetzung: Secret photonestudios/dropbox (app_key/app_secret/refresh_token), siehe setup_secret.py
set -euo pipefail
cd "$(dirname "$0")"
R=eu-west-1; A=$(aws sts get-caller-identity --query Account --output text)
FN=photon-dropbox-ingest; ROLE=photon-dropbox-ingest-role; SECRET=photonestudios/dropbox
B=${PHOTON_BUCKET:-photonestudios-photos}; T=${PHOTON_TABLE:-photonestudios-photos}
export AWS_REGION=$R AWS_PAGER=""
echo "  Account $A · Bucket $B · Tabelle $T"

SECRET_ARN=$(aws secretsmanager describe-secret --secret-id "$SECRET" --query ARN --output text) \
  || { echo "✗ Secret $SECRET fehlt (setup_secret.py)"; exit 1; }

echo "▸ 1/3 Rolle $ROLE"
aws iam create-role --role-name $ROLE --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' --tags Key=project,Value=photon >/dev/null 2>&1 && sleep 10 || echo "  (Rolle existiert schon)"
aws iam put-role-policy --role-name $ROLE --policy-name dropbox-ingest --policy-document "{
 \"Version\":\"2012-10-17\",\"Statement\":[
  {\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:AbortMultipartUpload\"],\"Resource\":\"arn:aws:s3:::${B}/*\"},
  {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:GetItem\",\"dynamodb:PutItem\",\"dynamodb:UpdateItem\",\"dynamodb:DeleteItem\"],\"Resource\":\"arn:aws:dynamodb:${R}:${A}:table/${T}\"},
  {\"Effect\":\"Allow\",\"Action\":[\"secretsmanager:GetSecretValue\"],\"Resource\":\"${SECRET_ARN}\"},
  {\"Effect\":\"Allow\",\"Action\":[\"logs:CreateLogGroup\",\"logs:CreateLogStream\",\"logs:PutLogEvents\"],\"Resource\":\"arn:aws:logs:${R}:${A}:*\"}
 ]}"

echo "▸ 2/3 Lambda $FN"
mkdir -p ../build && rm -f ../build/dropbox_ingest.zip && zip -qj ../build/dropbox_ingest.zip handler.py
ENV="Variables={PHOTON_BUCKET=$B,PHOTON_TABLE=$T,PHOTON_INGEST_WORKERS=12,PHOTON_LARGE_WORKERS=4,PHOTON_SAFETY_MS=240000}"
if aws lambda get-function --function-name $FN >/dev/null 2>&1; then
  aws lambda update-function-code --function-name $FN --zip-file fileb://../build/dropbox_ingest.zip >/dev/null
  aws lambda wait function-updated-v2 --function-name $FN
  aws lambda update-function-configuration --function-name $FN --environment "$ENV" >/dev/null
else
  aws lambda create-function --function-name $FN --runtime python3.12 --architectures arm64 \
    --handler handler.handler --role "arn:aws:iam::${A}:role/${ROLE}" --timeout 900 --memory-size 2048 \
    --ephemeral-storage Size=8192 --environment "$ENV" --tags project=photon \
    --zip-file fileb://../build/dropbox_ingest.zip >/dev/null
fi
aws lambda wait function-active-v2 --function-name $FN

echo "▸ 3/3 Nie zwei Läufe gleichzeitig (reserved concurrency 1)"
aws lambda put-function-concurrency --function-name $FN --reserved-concurrent-executions 1 >/dev/null
echo "✅ Fertig. Kein Zeitplan – Start manuell (siehe Kopf)."
