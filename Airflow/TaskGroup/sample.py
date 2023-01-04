from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
import pendulum
from utils.common.operation import convert_variable_to_list
from airflow.operators.python import PythonOperator

_local_tz = pendulum.timezone("Asia/Seoul")
_load_table_list = convert_variable_to_list(Variable.get('export_table_to_df'))

def test():
    return 'test'

def print_xcom(str_):
    print(str_)
    return None

with DAG(
    dag_id='test-dag-v1',
    schedule_interval="00 01 * * *",
    start_date=datetime(2022, 12, 28, tzinfo=_local_tz)
) as dag:
    task_group_id = ['A', 'B']
    task_group = []
    start = DummyOperator(
        task_id='start'
    )
    for group in task_group_id:
        with TaskGroup(group_id=f'group_{group}') as tg:
            export_s3 = PythonOperator(
                task_id='export_s3',
                python_callable=test,
                provide_context=True
            )
            sub_task_group = []
            for table_list in _load_table_list:
                with TaskGroup(group_id=f'loading_{table_list}') as sub_tg:
                    s3_load = DummyOperator(
                        task_id='s3_load'
                    )
                    task_id = f'group_{group}.export_s3'
                    print_str = PythonOperator(
                        task_id = 'print_test',
                        python_callable=print_xcom,
                        op_kwargs={
                            'str_': f"{{{{ ti.xcom_pull(task_ids='group_{group}.export_s3') }}}}"
                        }
                    )
                    truncate_real_table = DummyOperator(
                        task_id='truncate_real_table'
                    )

                    s3_load >> print_str >> truncate_real_table
                    sub_task_group.append(sub_tg)
            export_s3 >> sub_task_group
            task_group.append(tg)

    unload_redshift_s3 = DummyOperator(
        task_id='unload_redshift_s3'
    )

    send_finish_msg = DummyOperator(
        task_id='finished'
    )

    start >> task_group >> unload_redshift_s3 >> send_finish_msg