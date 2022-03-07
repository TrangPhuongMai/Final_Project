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
# from pyspark.sql import SparkSession
import sys
from random import random
from operator import add
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("spark", default_args=default_args, schedule_interval=timedelta(1), catchup=False, )

t1 = BashOperator(
    task_id="bash_task",
    bash_command="echo \"here is the message: '$message'\"",
    env={"message": '{{ dag_run.conf["message"] if dag_run else "" }}'},
    dag=dag
)

t2 = BashOperator(
    task_id="t2",
    bash_command="""
    /home/hdoop/spark-3.1.3-bin-hadoop3.2/bin/spark-submit --master yarn --queue dev  \
    --class SparkPi /home/hdoop/Final_Project/Test_main/target/scala-2.12/test_main_2.12-0.1.0-SNAPSHOT.jar \"$message\"
    """,
    env={"message": '{{ dag_run.conf["message"] if dag_run else "" }}'},
    dag=dag,
)

# UserSegment
t3 = BashOperator(
    task_id="t3",
    bash_command="""
    /home/hdoop/spark-3.1.3-bin-hadoop3.2/bin/spark-submit --master yarn --class UserSegment \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --queue dev \
    /home/hdoop/Final_Project/Spark_Final_Project/target/scala-2.12/spark_final_project_2.12-0.1.0-SNAPSHOT.jar \"$date\"
    """,
    env={"date": '{{ dag_run.conf["date"] if dag_run else "" }}'},
    dag=dag,
)

# /home/hdoop/spark-3.1.3-bin-hadoop3.2/bin/spark-submit --master localhost --class UserSegment /home/hdoop/Downloads/spark_final_project_2.12-0.1.0-SNAPSHOT.jar

# /home/hdo
# op/spark-3.1.3-bin-hadoop3.2/bin/spark-submit --master yarn --queue dev  --class UserSegment /home/hdoop/Downloads/spark_final_project_2.12-0.1.0-SNAPSHOT.jar


t1 >> t2
t3

# {"message":12,"date":"2021-11-02"}
# airflow dags trigger 'spark' --conf '{"message":12}'
#      /home/hdoop/spark-3.1.3-bin-hadoop3.2/bin/spark-submit --master yarn --queue dev  \
#     --class SparkPi /home/hdoop/Final_Project/Test_main/target/scala-2.12/test_main_2.12-0.1.0-SNAPSHOT.jar 12
