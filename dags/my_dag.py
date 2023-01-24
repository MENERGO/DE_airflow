from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def write_nums():
    from random import randint
    first_number = randint(0, 9)
    second_number = randint(0, 9)
    with open('numbers.txt', 'a', encoding='utf-8') as file:
        file.write(f'{first_number} {second_number}\n')
    return 'В файл записано два случайных числа'


def read_nums():
    first_row = 0
    second_row = 0
    with open('numbers.txt', 'r', encoding='utf-8') as file:
        for string in file.readlines():
            xy = string.rstrip('\n').split(' ')
            if len(xy) == 1:
                x = int(xy[0])
                y = 0
            else:
                x = int(xy[0])
                y = int(xy[1])
            first_row += x
            second_row += y
    difference = first_row - second_row
    with open('numbers.txt', 'a', encoding='utf-8') as file:
        file.write(f'{difference}\n')
    return f'Разность = {difference}'


default_args = {
    "start_date": datetime(2023, 1, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    }

with DAG(dag_id="task_dag",
         default_args=default_args,
         schedule_interval='1-5 * * * *',
         max_active_runs=5
         ) as dag:

    start = EmptyOperator(task_id="start")

    generate_numbers = PythonOperator(
        task_id='generate_numbers',
        python_callable=write_nums,
        dag=dag)

    read_numbers = PythonOperator(
        task_id='read_numbers',
        python_callable=read_nums,
        dag=dag)

    end = EmptyOperator(task_id="end")

    start >> generate_numbers >> read_numbers >> end
