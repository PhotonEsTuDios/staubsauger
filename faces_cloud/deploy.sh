#!/usr/bin/env bash
# Deploy photon-faces (staubsauger#14) — EINZIGER Weg in die Cloud, idempotent, alle Komponenten hier:
#   DynamoDB photonestudios-faces (+GSI cluster-index) · SQS photon-faces (+DLQ) · IAM-Rolle · Lambda · SQS-Trigger · 2 Zeitpläne
#   AWS_PROFILE=photon-new PYTHON=~/PhotonEsTuDios/.venv/bin/python ./deploy.sh
# Optionen: START=0 (nur einrichten) · LIMIT=50 (Pilot) · CLUSTER=1 (danach gruppieren) · PARALLEL=10
set -euo pipefail
cd "$(dirname "$0")"
R=eu-west-1; FN=photon-faces; ROLE=photon-faces-role; Q=photon-faces; DLQ=photon-faces-dlq
B=${PHOTON_BUCKET:-photonestudios-archive}; T=${PHOTON_TABLE:-photonestudios-photos}; FT=photonestudios-faces
PAR=${PARALLEL:-10}
export AWS_REGION=$R AWS_PAGER=""
A=$(aws sts get-caller-identity --query Account --output text) || { echo "✗ Keine AWS-Zugangsdaten"; exit 1; }
[ "$A" = 696682813662 ] || { echo "✗ Falscher Account $A (erwartet photon 696682813662)"; exit 1; }
echo "  Account $A, Bucket $B, Tabellen $T + $FT"

echo "▸ 0/7 Tests"
"${PYTHON:-python3}" -m pytest -q tests >/dev/null || { echo "✗ Tests rot, kein Deploy"; exit 1; }

echo "▸ 1/7 Tabelle $FT"
if ! aws dynamodb describe-table --table-name $FT >/dev/null 2>&1; then
  aws dynamodb create-table --table-name $FT --billing-mode PAY_PER_REQUEST \
    --attribute-definitions AttributeName=sha256,AttributeType=S AttributeName=face,AttributeType=N \
    --key-schema AttributeName=sha256,KeyType=HASH AttributeName=face,KeyType=RANGE \
    --tags Key=project,Value=photon >/dev/null
  aws dynamodb wait table-exists --table-name $FT
fi
# Index nach Gruppe (für Benennen aus der Gallery, gallery#7)
if ! aws dynamodb describe-table --table-name $FT --query 'Table.GlobalSecondaryIndexes[].IndexName' --output text | grep -qw cluster-index; then
  echo "  + Index cluster-index (Aufbau dauert ein paar Minuten)"
  aws dynamodb update-table --table-name $FT --attribute-definitions AttributeName=cluster,AttributeType=S \
    --global-secondary-index-updates '[{"Create":{"IndexName":"cluster-index","KeySchema":[{"AttributeName":"cluster","KeyType":"HASH"}],"Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":["person","crop","ignored","box","score","px"]}}}]' >/dev/null
fi
until [ "$(aws dynamodb describe-table --table-name $FT --query "Table.GlobalSecondaryIndexes[?IndexName=='cluster-index'].IndexStatus" --output text)" = ACTIVE ]; do sleep 15; done

echo "▸ 2/7 SQS-Queues"
aws sqs create-queue --queue-name $DLQ --attributes MessageRetentionPeriod=1209600 >/dev/null
Q_URL=$(aws sqs create-queue --queue-name $Q --attributes "{\"VisibilityTimeout\":\"5400\",\"MessageRetentionPeriod\":\"1209600\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"arn:aws:sqs:$R:$A:$DLQ\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}" --query QueueUrl --output text)
Q_ARN="arn:aws:sqs:$R:$A:$Q"

echo "▸ 3/7 Rolle $ROLE"
aws iam create-role --role-name $ROLE --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null 2>&1 && sleep 10 || echo "  (Rolle existiert schon)"
aws iam put-role-policy --role-name $ROLE --policy-name faces --policy-document "{
 \"Version\":\"2012-10-17\",\"Statement\":[
  {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\"],\"Resource\":\"arn:aws:s3:::$B/*\"},
  {\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":\"arn:aws:s3:::$B/faces/*\"},
  {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:Scan\",\"dynamodb:Query\",\"dynamodb:GetItem\",\"dynamodb:BatchGetItem\",\"dynamodb:PutItem\",\"dynamodb:UpdateItem\"],\"Resource\":[\"arn:aws:dynamodb:$R:$A:table/$T\",\"arn:aws:dynamodb:$R:$A:table/$FT\",\"arn:aws:dynamodb:$R:$A:table/$FT/index/*\"]},
  {\"Effect\":\"Allow\",\"Action\":[\"sqs:SendMessage\",\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:GetQueueAttributes\",\"sqs:ChangeMessageVisibility\"],\"Resource\":\"$Q_ARN\"},
  {\"Effect\":\"Allow\",\"Action\":[\"logs:CreateLogGroup\",\"logs:CreateLogStream\",\"logs:PutLogEvents\"],\"Resource\":\"arn:aws:logs:$R:$A:*\"}
 ]}"

echo "▸ 4/7 Build + Lambda $FN"
ZIP=$(PYTHON="${PYTHON:-python3}" ./build.sh)
aws s3 cp "$ZIP" "s3://$B/build/faces_cloud.zip" --quiet
ENV="Variables={PHOTON_QUEUE_URL=$Q_URL,PHOTON_BUCKET=$B,PHOTON_TABLE=$T,PHOTON_FACES_TABLE=$FT}"
if aws lambda get-function --function-name $FN >/dev/null 2>&1; then
  aws lambda update-function-code --function-name $FN --s3-bucket $B --s3-key build/faces_cloud.zip >/dev/null
  aws lambda wait function-updated-v2 --function-name $FN
  aws lambda update-function-configuration --function-name $FN --environment "$ENV" --timeout 900 --memory-size 3008 >/dev/null
else
  aws lambda create-function --function-name $FN --runtime python3.12 --architectures arm64 \
    --handler handler.handler --role arn:aws:iam::$A:role/$ROLE --timeout 900 --memory-size 3008 \
    --environment "$ENV" --code S3Bucket=$B,S3Key=build/faces_cloud.zip --tags project=photon >/dev/null
fi
aws lambda wait function-active-v2 --function-name $FN
aws lambda wait function-updated-v2 --function-name $FN

echo "▸ 5/7 SQS-Trigger ($PAR parallel)"
UUID=$(aws lambda list-event-source-mappings --function-name $FN --event-source-arn $Q_ARN --query 'EventSourceMappings[0].UUID' --output text)
if [ "$UUID" = None ] || [ -z "$UUID" ]; then
  aws lambda create-event-source-mapping --function-name $FN --event-source-arn $Q_ARN --batch-size 1 \
    --function-response-types ReportBatchItemFailures --scaling-config MaximumConcurrency=$PAR >/dev/null
else
  aws lambda update-event-source-mapping --uuid "$UUID" --scaling-config MaximumConcurrency=$PAR >/dev/null
fi

echo "▸ 6/7 Zeitpläne (Plan 04:30 UTC nach dem Labeler, Gruppieren 06:00 UTC)"
for rule in "plan|cron(30 4 * * ? *)" "cluster|cron(0 6 * * ? *)"; do
  m=${rule%%|*}; cron=${rule#*|}; name=photon-faces-$m
  aws events put-rule --name $name --schedule-expression "$cron" >/dev/null
  aws lambda add-permission --function-name $FN --statement-id events-$m --action lambda:InvokeFunction \
    --principal events.amazonaws.com --source-arn "arn:aws:events:$R:$A:rule/$name" >/dev/null 2>&1 || true
  aws events put-targets --rule $name --targets "Id=faces,Arn=arn:aws:lambda:$R:$A:function:$FN,Input='{\"mode\":\"$m\"}'" >/dev/null
done

[ "${START:-1}" = 1 ] || { echo "✅ Eingerichtet, kein Lauf gestartet (START=0)"; exit 0; }
echo "▸ 7/7 Plan starten${LIMIT:+ (Pilot: $LIMIT)}"
aws lambda invoke --function-name $FN --cli-binary-format raw-in-base64-out \
  --payload "{\"mode\":\"plan\",\"limit\":${LIMIT:-0}}" /dev/stdout 2>/dev/null | head -c 300; echo
echo "✅ Läuft in der Cloud. Stand: AWS_PROFILE=$AWS_PROFILE ${PYTHON:-python3} status.py"
