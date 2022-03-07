#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG to submit Apache Spark applications using
`SparkSubmitOperator`, `SparkJDBCOperator` and `SparkSqlOperator`.
"""
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from datetime import date

import datetime
import pendulum

default_args = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2021, 11, 1, tz="UTC"),
    "retry_delay": timedelta(minutes=5),
    'params': {
        "date": str(date.today())
    }
}

dag = DAG("dags-final", default_args=default_args,
          schedule_interval="@daily",
          catchup=True,
          params={"date": str(date.today())}, )

t1 = BashOperator(
    task_id="log",
    bash_command="""
    echo \"'$date'\"
    """,
    env={"date": '{{ dag_run.conf["date"] if dag_run.conf.get("date") else ds }}'},
    dag=dag
)
# UserSegment
t3 = BashOperator(
    task_id="t3",
    bash_command="""
    /home/hdoop/spark-3.1.3-bin-hadoop3.2/bin/spark-submit --master yarn --class UserSegment \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1  --queue dev \
    /home/hdoop/Final_Project/Spark_Final_Project/target/scala-2.12/spark_final_project_2.12-0.1.0-SNAPSHOT.jar \
    \"$date\" \n""",
    env={"date": '{{ dag_run.conf["date"] if dag_run.conf.get("date") else ds }}'},
    dag=dag,
)

t1 >> t3
