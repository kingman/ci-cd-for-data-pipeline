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
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.operators.python_operator import PythonOperator

dataflow_staging_bucket = 'gs://' \
    + models.Variable.get('dataflow_staging_bucket_test') \
    + '/staging'
dataflow_jar_location = 'gs://' \
    + models.Variable.get('dataflow_jar_location_test') \
    + '/' + models.Variable.get('dataflow_jar_file_test')
project = models.Variable.get('gcp_project')
input_bucket = 'gs://' + models.Variable.get('gcs_input_bucket_test')
output_bucket_name = models.Variable.get('gcs_output_bucket_test')
output_bucket = 'gs://' + output_bucket_name
ref_bucket = models.Variable.get('gcs_ref_bucket_test')
output_prefix = 'output'
download_task_prefix = 'download_result'

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

def parse_str_obj(str_rep, obj):
    entries = str_rep.split('\n')
    for entry in entries:
        if entry:
            key,value = entry.split(': ')
            obj[key]=value

def compare_obj(ref_obj, res_obj):
    if cmp(ref_obj, res_obj) != 0 :
        raise ValueError('Test result does not match the expected result')

def read_value_as_obj(task_ids, context):
    ret_obj = {}
    for task_id in task_ids:
        value_str = context['ti'].xcom_pull(
            key=None,
            task_ids=task_id)
        parse_str_obj(value_str, ret_obj)
    return ret_obj

def verify_test_result(ref_task_id, res_task_ids, **context):
    ref_obj = read_value_as_obj([ref_task_id], context)
    res_obj = read_value_as_obj(res_task_ids, context)
    compare_obj(ref_obj, res_obj)
    return 'result contains the expected values'


default_args = {
    'dataflow_default_options': {
        'project': project,
        'zone': 'europe-west1-b',
        'region': 'europe-west1',
        'stagingLocation': dataflow_staging_bucket
    }
}

with models.DAG(
        'test_word_count',
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
    download_expected = GoogleCloudStorageDownloadOperator(
        task_id='download_ref_string',
        bucket=ref_bucket,
        object='ref.txt',
        store_to_xcom_key='ref_str',
        start_date=yesterday
    )
    download_result_one = GoogleCloudStorageDownloadOperator(
        task_id=download_task_prefix+'_1',
        bucket=output_bucket_name,
        object=output_prefix+'-00000-of-00003',
        store_to_xcom_key='res_str_1',
        start_date=yesterday
    )
    download_result_two = GoogleCloudStorageDownloadOperator(
        task_id=download_task_prefix+'_2',
        bucket=output_bucket_name,
        object=output_prefix+'-00001-of-00003',
        store_to_xcom_key='res_str_2',
        start_date=yesterday
    )
    download_result_three = GoogleCloudStorageDownloadOperator(
        task_id=download_task_prefix+'_3',
        bucket=output_bucket_name,
        object=output_prefix+'-00002-of-00003',
        store_to_xcom_key='res_str_3',
        start_date=yesterday
    )
    compare_result = PythonOperator(
        task_id='do_comparison',
        provide_context=True,
        python_callable=verify_test_result,
        op_kwargs={
            'ref_task_id': 'download_ref_string',
            'res_task_ids': [download_task_prefix+'_1', download_task_prefix+'_2', download_task_prefix+'_3']
        },
        start_date=yesterday
    )

    dataflow_execution >> download_result_one
    dataflow_execution >> download_result_two
    dataflow_execution >> download_result_three

    download_expected >> compare_result
    download_result_one >> compare_result
    download_result_two >> compare_result
    download_result_three >> compare_result
