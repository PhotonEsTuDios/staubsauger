#!/usr/bin/env bash
# Deploy photon-icloud (staubsauger#2) — EINZIGER Weg in die Cloud, idempotent, alle Komponenten hier:
#   SQS photon-icloud (+DLQ) · 2 IAM-Rollen · Lambdas photon-icloud-plan + photon-icloud-worker
#   · SQS-Trigger · Zeitplan alle 15 Minuten (erst nach Freigabe: SCHEDULE=1)
#   AWS_PROFILE=photon-new PYTHON=~/PhotonEsTuDios/.venv/bin/python ./deploy.sh
# Optionen: RUN=dry (nur zählen, Standard) · RUN=full · RUN=0 (nichts starten) · LIMIT=50 · PARALLEL=8 · SCHEDULE=1
# Vorher einmal: login.py (legt Secret photonestudios/icloud an).
set -euo pipefail
cd "$(dirname "$0")"
R=eu-west-1; P=photon-icloud-plan; W=photon-icloud-worker; Q=photon-icloud; DLQ=photon-icloud-dlq
B=${PHOTON_BUCKET:-photonestudios-archive}; T=${PHOTON_TABLE:-photonestudios-photos}; S=photonestudios/icloud
PAR=${PARALLEL:-8}; RUN=${RUN:-dry}
export AWS_REGION=$R AWS_PAGER=""
A=$(aws sts get-caller-identity --query Account --output text) || { echo "✗ Keine AWS-Zugangsdaten"; exit 1; }
[ "$A" = 696682813662 ] || { echo "✗ Falscher Account $A (erwartet photon 696682813662)"; exit 1; }
echo "  Account $A, Bucket $B, Tabelle $T"

echo "▸ 0/7 Tests"
"${PYTHON:-python3}" -m pytest -q tests >/dev/null || { echo "✗ Tests rot, kein Deploy"; exit 1; }

echo "▸ 1/7 SQS-Queues"
aws sqs create-queue --queue-name $DLQ --attributes MessageRetentionPeriod=1209600 >/dev/null
Q_URL=$(aws sqs create-queue --queue-name $Q --attributes "{\"VisibilityTimeout\":\"5400\",\"MessageRetentionPeriod\":\"345600\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"arn:aws:sqs:$R:$A:$DLQ\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}" --query QueueUrl --output text)
Q_ARN="arn:aws:sqs:$R:$A:$Q"

echo "▸ 2/7 Rollen"
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
LOGS="{\"Effect\":\"Allow\",\"Action\":[\"logs:CreateLogGroup\",\"logs:CreateLogStream\",\"logs:PutLogEvents\"],\"Resource\":\"arn:aws:logs:$R:$A:*\"}"
NEW=0
for role in $P-role $W-role; do
  aws iam create-role --role-name $role --assume-role-policy-document "$TRUST" --tags Key=project,Value=photon >/dev/null 2>&1 && NEW=1 || true
done
# Plan: darf die iCloud-Sitzung lesen/zurückschreiben, Tabelle lesen, Queue füllen, sich selbst fortsetzen.
aws iam put-role-policy --role-name $P-role --policy-name icloud-plan --policy-document "{
 \"Version\":\"2012-10-17\",\"Statement\":[
  {\"Effect\":\"Allow\",\"Action\":[\"secretsmanager:GetSecretValue\",\"secretsmanager:PutSecretValue\"],\"Resource\":\"arn:aws:secretsmanager:$R:$A:secret:$S-*\"},
  {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:Scan\",\"dynamodb:GetItem\",\"dynamodb:PutItem\",\"dynamodb:UpdateItem\"],\"Resource\":\"arn:aws:dynamodb:$R:$A:table/$T\"},
  {\"Effect\":\"Allow\",\"Action\":[\"sqs:SendMessage\"],\"Resource\":\"$Q_ARN\"},
  {\"Effect\":\"Allow\",\"Action\":[\"lambda:InvokeFunction\"],\"Resource\":\"arn:aws:lambda:$R:$A:function:$P\"},
  $LOGS ]}"
# Worker: KEIN Secret — lädt nur über die Download-Links aus der Queue.
aws iam put-role-policy --role-name $W-role --policy-name icloud-worker --policy-document "{
 \"Version\":\"2012-10-17\",\"Statement\":[
  {\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\"],\"Resource\":\"arn:aws:s3:::$B/*\"},
  {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:GetItem\",\"dynamodb:PutItem\",\"dynamodb:UpdateItem\"],\"Resource\":\"arn:aws:dynamodb:$R:$A:table/$T\"},
  {\"Effect\":\"Allow\",\"Action\":[\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:GetQueueAttributes\",\"sqs:ChangeMessageVisibility\"],\"Resource\":\"$Q_ARN\"},
  $LOGS ]}"
[ $NEW = 1 ] && sleep 10  # IAM-Propagation

echo "▸ 3/7 Build"
ZIP=$(PYTHON="${PYTHON:-python3}" ./build.sh)
aws s3 cp "$ZIP" "s3://$B/build/icloud_cloud.zip" --quiet
ENV="Variables={PHOTON_QUEUE_URL=$Q_URL,PHOTON_BUCKET=$B,PHOTON_TABLE=$T,PHOTON_ICLOUD_SECRET=$S}"

deploy_fn() {  # name handler memory ephemeral
  if aws lambda get-function --function-name $1 >/dev/null 2>&1; then
    aws lambda update-function-code --function-name $1 --s3-bucket $B --s3-key build/icloud_cloud.zip >/dev/null
    aws lambda wait function-updated-v2 --function-name $1
    aws lambda update-function-configuration --function-name $1 --handler $2 --environment "$ENV" \
      --timeout 900 --memory-size $3 --ephemeral-storage Size=$4 >/dev/null
  else
    aws lambda create-function --function-name $1 --runtime python3.12 --architectures arm64 \
      --handler $2 --role arn:aws:iam::$A:role/$1-role --timeout 900 --memory-size $3 \
      --ephemeral-storage Size=$4 --environment "$ENV" \
      --code S3Bucket=$B,S3Key=build/icloud_cloud.zip --tags project=photon >/dev/null
  fi
  aws lambda wait function-active-v2 --function-name $1
  aws lambda wait function-updated-v2 --function-name $1
}
echo "▸ 4/7 Lambda $P (1 gleichzeitig)"
deploy_fn $P handler.plan_handler 1024 1024
aws lambda put-function-concurrency --function-name $P --reserved-concurrent-executions 1 >/dev/null
echo "▸ 5/7 Lambda $W + SQS-Trigger ($PAR parallel)"
deploy_fn $W handler.worker_handler 1024 10240
UUID=$(aws lambda list-event-source-mappings --function-name $W --event-source-arn $Q_ARN --query 'EventSourceMappings[0].UUID' --output text)
if [ "$UUID" = None ] || [ -z "$UUID" ]; then
  aws lambda create-event-source-mapping --function-name $W --event-source-arn $Q_ARN --batch-size 1 \
    --function-response-types ReportBatchItemFailures --scaling-config MaximumConcurrency=$PAR >/dev/null
else
  aws lambda update-event-source-mapping --uuid "$UUID" --scaling-config MaximumConcurrency=$PAR >/dev/null
fi

echo "▸ 6/7 Zeitplan alle 15 Minuten ($([ "${SCHEDULE:-0}" = 1 ] && echo AN || echo 'AUS — SCHEDULE=1 nach Freigabe'))"
aws events put-rule --name photon-icloud-daily --schedule-expression "rate(15 minutes)" \
  --state $([ "${SCHEDULE:-0}" = 1 ] && echo ENABLED || echo DISABLED) >/dev/null
aws lambda add-permission --function-name $P --statement-id events-daily --action lambda:InvokeFunction \
  --principal events.amazonaws.com --source-arn "arn:aws:events:$R:$A:rule/photon-icloud-daily" >/dev/null 2>&1 || true
aws events put-targets --rule photon-icloud-daily --targets "Id=plan,Arn=arn:aws:lambda:$R:$A:function:$P,Input='{}'" >/dev/null

case $RUN in
  0) echo "✅ Eingerichtet, kein Lauf gestartet (RUN=0)"; exit 0;;
  dry) PAY="{\"dry_run\":true,\"limit\":${LIMIT:-0}}";;
  full) PAY="{\"limit\":${LIMIT:-0}}";;
  *) echo "✗ RUN=$RUN unbekannt"; exit 1;;
esac
echo "▸ 7/7 Plan starten ($RUN${LIMIT:+, Limit $LIMIT})"
aws lambda invoke --function-name $P --invocation-type Event --cli-binary-format raw-in-base64-out \
  --payload "$PAY" /dev/null >/dev/null
echo "✅ Läuft in der Cloud. Stand: AWS_PROFILE=$AWS_PROFILE ${PYTHON:-python3} status.py"
