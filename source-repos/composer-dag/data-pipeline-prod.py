# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator

dataflow_staging_bucket = 'gs://' \
    + models.Variable.get('dataflow_staging_bucket_prod') \
    + '/staging'
dataflow_jar_location = 'gs://' \
    + models.Variable.get('dataflow_jar_location_prod') \
    + '/' + models.Variable.get('dataflow_jar_file_prod')
project = models.Variable.get('gcp_project')
input_bucket = 'gs://' + models.Variable.get('gcs_input_bucket_prod')
output_bucket_name = models.Variable.get('gcs_output_bucket_prod')
output_bucket = 'gs://' + output_bucket_name
output_prefix = 'output'
download_task_prefix = 'download_result'

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_args = {
    'dataflow_default_options': {
        'project': project,
        'zone': 'europe-west1-b',
        'region': 'europe-west1',
        'stagingLocation': dataflow_staging_bucket
    }
}

with models.DAG(
        'prod_word_count',
        schedule_interval=None,
        default_args=default_args) as dag:
    dataflow_execution = DataFlowJavaOperator(
        task_id='wordcount-run',
        jar=dataflow_jar_location,
        start_date=yesterday,
        options={
            'autoscalingAlgorithm': 'BASIC',
            'maxNumWorkers': '50',
            'inputFile': input_bucket+'/input.txt',
            'output': output_bucket+'/'+output_prefix
        }
    )
