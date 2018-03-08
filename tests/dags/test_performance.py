# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'catchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}
dag = DAG(
    "performance_4_fans", default_args=default_args, schedule_interval=None)


def create_print_hello(dag, count_print_hello, suffix):
    return BashOperator(
        task_id='print_hello_%s_%s' % (count_print_hello, suffix),
        bash_command='echo "hello world!"',
        dag=dag)


begin_task = BashOperator(
    task_id='begin_task',
    bash_command='echo "Start here"',
    dag=dag)
hold1 = BashOperator(
    task_id='hold1',
    bash_command='echo "Start here"',
    dag=dag)
hold2 = BashOperator(
    task_id='hold2',
    bash_command='echo "Start here"',
    dag=dag)
hold3 = BashOperator(
    task_id='hold3',
    bash_command='echo "Start here"',
    dag=dag)
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "Start here"',
    dag=dag)

width = 500
for i in range(width):
    print1 = create_print_hello(dag, i, 'first')
    print2 = create_print_hello(dag, i, 'second')
    print3 = create_print_hello(dag, i, 'third')
    print4 = create_print_hello(dag, i, 'fourth')

    begin_task.set_downstream(print1)
    print1.set_downstream(hold1)

    hold1.set_downstream(print2)
    print2.set_downstream(hold2)

    hold2.set_downstream(print3)
    print3.set_downstream(hold3)

    hold3.set_downstream(print4)
    print4.set_downstream(end_task)
