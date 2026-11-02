#!/usr/bin/env bash
# Legt photon-labeler an (idempotent) und startet den Backfill (staubsauger#6).
# Komplett in der Cloud: Plan → SQS → Worker (parallel). Täglich 04:00 UTC neuer Plan für neue Fotos.
#   AWS_PROFILE=photon-new PHOTON_BUCKET=photonestudios-archive PYTHON=~/PhotonEsTuDios/.venv/bin/python ./setup-aws.sh
# Ohne Start: START=0 ./setup-aws.sh     Testlauf mit wenigen: LIMIT=50 ./setup-aws.sh
set -euo pipefail
cd "$(dirname "$0")"
R=eu-west-1; FN=photon-labeler; ROLE=photon-labeler-role; Q=photon-labeler; DLQ=photon-labeler-dlq
B=${PHOTON_BUCKET:-photonestudios-archive}; T=${PHOTON_TABLE:-photonestudios-photos}; PAR=${PARALLEL:-20}
export AWS_REGION=$R AWS_PAGER=""
A=$(aws sts get-caller-identity --query Account --output text) || { echo "✗ Keine AWS-Zugangsdaten"; exit 1; }
echo "  Account $A, Bucket $B, Tabelle $T"

echo "▸ 1/6 SQS-Queues"
aws sqs create-queue --queue-name $DLQ --attributes MessageRetentionPeriod=1209600 >/dev/null
Q_URL=$(aws sqs create-queue --queue-name $Q --attributes "{\"VisibilityTimeout\":\"900\",\"MessageRetentionPeriod\":\"1209600\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"arn:aws:sqs:$R:$A:$DLQ\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}" --query QueueUrl --output text)
Q_ARN="arn:aws:sqs:$R:$A:$Q"

echo "▸ 2/6 Rolle $ROLE"
aws iam create-role --role-name $ROLE --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null 2>&1 || echo "  (Rolle existiert schon)"
aws iam put-role-policy --role-name $ROLE --policy-name labeler --policy-document "{
 \"Version\":\"2012-10-17\",\"Statement\":[
  {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\"],\"Resource\":\"arn:aws:s3:::$B/*\"},
  {\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":\"arn:aws:s3:::$B/thumbs/*\"},
  {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:Scan\",\"dynamodb:GetItem\",\"dynamodb:PutItem\",\"dynamodb:UpdateItem\"],\"Resource\":\"arn:aws:dynamodb:$R:$A:table/$T\"},
  {\"Effect\":\"Allow\",\"Action\":[\"sqs:SendMessage\",\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:GetQueueAttributes\",\"sqs:ChangeMessageVisibility\"],\"Resource\":\"$Q_ARN\"},
  {\"Effect\":\"Allow\",\"Action\":[\"logs:CreateLogGroup\",\"logs:CreateLogStream\",\"logs:PutLogEvents\"],\"Resource\":\"arn:aws:logs:$R:$A:*\"}
 ]}"

echo "▸ 3/6 ffmpeg-Layer"
LAYER=$(aws lambda list-layer-versions --layer-name photon-ffmpeg --query 'LayerVersions[0].LayerVersionArn' --output text 2>/dev/null || true)
if [ -z "$LAYER" ] || [ "$LAYER" = None ]; then LAYER=$(PHOTON_BUCKET=$B ./ffmpeg-layer.sh); fi
echo "  $LAYER"
sleep 10  # IAM-Propagation

echo "▸ 4/6 Lambda $FN"
ZIP=$(PYTHON="${PYTHON:-python3}" ./build.sh)
aws s3 cp "$ZIP" "s3://$B/build/labeler_cloud.zip" --quiet
ENV="Variables={PHOTON_QUEUE_URL=$Q_URL,PHOTON_BUCKET=$B,PHOTON_TABLE=$T}"
if aws lambda get-function --function-name $FN >/dev/null 2>&1; then
  aws lambda update-function-code --function-name $FN --s3-bucket $B --s3-key build/labeler_cloud.zip >/dev/null
  aws lambda wait function-updated-v2 --function-name $FN
  aws lambda update-function-configuration --function-name $FN --environment "$ENV" --layers "$LAYER" >/dev/null
else
  aws lambda create-function --function-name $FN --runtime python3.12 --architectures arm64 \
    --handler handler.handler --role arn:aws:iam::$A:role/$ROLE --timeout 600 --memory-size 2048 \
    --environment "$ENV" --layers "$LAYER" --code S3Bucket=$B,S3Key=build/labeler_cloud.zip >/dev/null
fi
aws lambda wait function-active-v2 --function-name $FN
aws lambda wait function-updated-v2 --function-name $FN

echo "▸ 5/6 SQS-Trigger ($PAR parallel) + täglicher Plan"
aws lambda create-event-source-mapping --function-name $FN --event-source-arn $Q_ARN --batch-size 1 \
  --function-response-types ReportBatchItemFailures --scaling-config MaximumConcurrency=$PAR >/dev/null 2>&1 \
  || echo "  (Trigger existiert schon)"
aws events put-rule --name photon-labeler-daily --schedule-expression "cron(0 4 * * ? *)" >/dev/null
aws lambda add-permission --function-name $FN --statement-id events-daily --action lambda:InvokeFunction \
  --principal events.amazonaws.com --source-arn "arn:aws:events:$R:$A:rule/photon-labeler-daily" >/dev/null 2>&1 || true
aws events put-targets --rule photon-labeler-daily --targets "Id=labeler,Arn=arn:aws:lambda:$R:$A:function:$FN,Input='{\"mode\":\"plan\"}'" >/dev/null

[ "${START:-1}" = 1 ] || { echo "✅ Eingerichtet, Plan NICHT gestartet (START=0)"; exit 0; }
echo "▸ 6/6 Plan starten${LIMIT:+ (Test: $LIMIT)}"
aws lambda invoke --function-name $FN --cli-binary-format raw-in-base64-out \
  --payload "{\"mode\":\"plan\",\"limit\":${LIMIT:-0}}" /dev/stdout 2>/dev/null | head -c 300; echo
echo "✅ Läuft in der Cloud. Stand: AWS_PROFILE=$AWS_PROFILE python status.py"
