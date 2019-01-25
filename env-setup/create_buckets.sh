#!/bin/bash
# Copyright Google Inc. 2019

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

gsutil ls -L gs://$DATAFLOW_JAR_BUCKET_TEST || gsutil mb -c regional -l $COMPOSER_REGION "gs://$DATAFLOW_JAR_BUCKET_TEST"
gsutil ls -L gs://$INPUT_BUCKET_TEST || gsutil mb -c regional -l $COMPOSER_REGION "gs://$INPUT_BUCKET_TEST"
gsutil ls -L gs://$REF_BUCKET_TEST || gsutil mb -c regional -l $COMPOSER_REGION "gs://$REF_BUCKET_TEST"
gsutil ls -L gs://$RESULT_BUCKET_TEST || gsutil mb -c regional -l $COMPOSER_REGION "gs://$RESULT_BUCKET_TEST"
gsutil ls -L gs://$DATAFLOW_STAGING_BUCKET_TEST || gsutil mb -c regional -l $COMPOSER_REGION "gs://$DATAFLOW_STAGING_BUCKET_TEST"
gsutil ls -L gs://$DATAFLOW_JAR_BUCKET_PROD || gsutil mb -c regional -l $COMPOSER_REGION "gs://$DATAFLOW_JAR_BUCKET_PROD"
gsutil ls -L gs://$INPUT_BUCKET_PROD || gsutil mb -c regional -l $COMPOSER_REGION "gs://$INPUT_BUCKET_PROD"
gsutil ls -L gs://$RESULT_BUCKET_PROD || gsutil mb -c regional -l $COMPOSER_REGION "gs://$RESULT_BUCKET_PROD"
gsutil ls -L gs://$DATAFLOW_STAGING_BUCKET_PROD || gsutil mb -c regional -l $COMPOSER_REGION "gs://$DATAFLOW_STAGING_BUCKET_PROD"

gsutil acl ch -u $COMPOSER_SERVICE_ACCOUNT:R gs://$DATAFLOW_JAR_BUCKET_TEST gs://$INPUT_BUCKET_TEST gs://$REF_BUCKET_TEST gs://$DATAFLOW_JAR_BUCKET_PROD gs://$INPUT_BUCKET_PROD
gsutil acl ch -u $COMPOSER_SERVICE_ACCOUNT:W gs://$RESULT_BUCKET_TEST gs://$DATAFLOW_STAGING_BUCKET_TEST gs://$RESULT_BUCKET_PROD gs://$DATAFLOW_STAGING_BUCKET_PROD
