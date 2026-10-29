#!/usr/bin/env bash
# Legt alles für photon-s3importer an (idempotent) und startet den Import.
# Läuft komplett in der Cloud: Plan-Aufruf → SQS → Worker (20 parallel). Der Mac stößt nur an.
#   AWS_PROFILE=photon-new PHOTON_BUCKET=photonestudios-archive PYTHON=~/PhotonEsTuDios/.venv/bin/python ./setup-aws.sh
# Ohne Start des Imports: START=0 ./setup-aws.sh
set -euo pipefail
cd "$(dirname "$0")"
R=eu-west-1; A=$(aws sts get-caller-identity --query Account --output text); FN=photon-s3importer; ROLE=photon-s3importer-role
Q=photon-s3import; DLQ=photon-s3import-dlq; B=${PHOTON_BUCKET:-photonestudios-photos}; T=${PHOTON_TABLE:-photonestudios-photos}
export AWS_REGION=$R AWS_PAGER=""
ME=$(aws sts get-caller-identity --query Arn --output text) || { echo "✗ Keine gültigen AWS-Zugangsdaten (AWS_PROFILE=${AWS_PROFILE:-})"; exit 1; }
echo "  angemeldet als $ME"

echo "▸ 1/5 SQS-Queues"
DLQ_URL=$(aws sqs create-queue --queue-name $DLQ --attributes MessageRetentionPeriod=1209600 --query QueueUrl --output text)
DLQ_ARN="arn:aws:sqs:$R:$A:$DLQ"
Q_URL=$(aws sqs create-queue --queue-name $Q --attributes "{\"VisibilityTimeout\":\"1000\",\"MessageRetentionPeriod\":\"1209600\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}" --query QueueUrl --output text)
Q_ARN="arn:aws:sqs:$R:$A:$Q"

echo "▸ 2/5 Rolle $ROLE"
aws iam create-role --role-name $ROLE --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null 2>&1 || echo "  (Rolle existiert schon)"
aws iam put-role-policy --role-name $ROLE --policy-name s3importer --policy-document "{
 \"Version\":\"2012-10-17\",\"Statement\":[
  {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\"],\"Resource\":\"arn:aws:s3:::$B/raw/*\"},
  {\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:AbortMultipartUpload\"],\"Resource\":\"arn:aws:s3:::$B/*\"},
  {\"Effect\":\"Allow\",\"Action\":[\"s3:ListBucket\"],\"Resource\":\"arn:aws:s3:::$B\",\"Condition\":{\"StringLike\":{\"s3:prefix\":[\"raw/*\"]}}},
  {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:GetItem\",\"dynamodb:PutItem\",\"dynamodb:UpdateItem\"],\"Resource\":\"arn:aws:dynamodb:$R:$A:table/$T\"},
  {\"Effect\":\"Allow\",\"Action\":[\"sqs:SendMessage\",\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:GetQueueAttributes\",\"sqs:ChangeMessageVisibility\"],\"Resource\":\"$Q_ARN\"},
  {\"Effect\":\"Allow\",\"Action\":[\"logs:CreateLogGroup\",\"logs:CreateLogStream\",\"logs:PutLogEvents\"],\"Resource\":\"arn:aws:logs:$R:$A:*\"}
 ]}"
sleep 12  # IAM-Propagation

echo "▸ 3/5 Lambda $FN"
ZIP=$(PYTHON="${PYTHON:-python3}" ./build.sh)
ENV="Variables={PHOTON_QUEUE_URL=$Q_URL,PHOTON_BUCKET=$B,PHOTON_TABLE=$T}"
if aws lambda get-function --function-name $FN >/dev/null 2>&1; then
  aws lambda update-function-code --function-name $FN --zip-file "fileb://$ZIP" >/dev/null
  aws lambda wait function-updated-v2 --function-name $FN
  aws lambda update-function-configuration --function-name $FN --environment "$ENV" >/dev/null
else
  aws lambda create-function --function-name $FN --runtime python3.12 --architectures arm64 \
    --handler handler.handler --role arn:aws:iam::$A:role/$ROLE --timeout 900 --memory-size 3008 \
    --ephemeral-storage Size=10240 --environment "$ENV" --zip-file "fileb://$ZIP" >/dev/null
fi
aws lambda wait function-active-v2 --function-name $FN

echo "▸ 4/5 SQS-Trigger (20 parallel)"
aws lambda create-event-source-mapping --function-name $FN --event-source-arn $Q_ARN --batch-size 1 \
  --function-response-types ReportBatchItemFailures --scaling-config MaximumConcurrency=20 >/dev/null 2>&1 \
  || echo "  (Trigger existiert schon)"

[ "${START:-1}" = 1 ] || { echo "✅ Eingerichtet, Import NICHT gestartet (START=0)"; exit 0; }
echo "▸ 5/5 Import starten (Plan-Aufruf, asynchron)"
aws lambda invoke --function-name $FN --invocation-type Event \
  --cli-binary-format raw-in-base64-out \
  --payload '{"mode":"plan","prefix":"raw/timemachine-2024-12-14/","source":"timemachine:2024-12-14"}' /dev/null >/dev/null
echo "✅ Fertig. Stand: Gallery → Import, oder: AWS_PROFILE=$AWS_PROFILE PHOTON_TABLE=$T python status.py"
