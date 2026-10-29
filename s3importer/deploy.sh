#!/usr/bin/env bash
# Code-Update für photon-s3importer (nach dem einmaligen setup-aws.sh).
set -euo pipefail
cd "$(dirname "$0")"
export AWS_PROFILE="${AWS_PROFILE:-photon}" AWS_REGION=eu-west-1
ZIP=$(./build.sh)
aws lambda update-function-code --function-name photon-s3importer --zip-file "fileb://$ZIP" --query '[CodeSize,LastModified]' --output text
