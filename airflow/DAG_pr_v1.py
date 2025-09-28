from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Константы подключения и конфигурации
DB_CONN = "gp_std12_116"
DB_SCHEMA = 'std12_116_pr'   

DB_PROC_LOAD = 'f_load_full'                                  # Имя хранимой процедуры для полной загрузки
FULL_LOAD_TABLES = ['stores', 'promo_types', 'promotions']    # Таблицы для полной загрузки
FULL_LOAD_FILES = {               #  имя таблицы : имя файла
    'stores': 'stores',
    'promo_types':'promo_types',
    'promotions': 'promotions'}

MD_TABLE_LOAD_QUERY = (                     #  SQL к хранимой процедуре полной загрузки
    f"select {DB_SCHEMA}.{DB_PROC_LOAD}(%(tabname)s,%(file_name)s);"
)
#  SELECT std12_116_pr.f_load_full('std12_116_pr.promotions', 'promotions');

DB_DELTA_PART = 'f_delta_partition'
DELTA_PART_TABLE = ['bills_head', 'bills_item']
DELTA_PART_KEYS = {
    'bills_head': 'calday',
    'bills_item': 'calday'}
DB_SCHEMA_FOR_LOAD = 'gp'
login_p = 'intern'
pass_p = 'intern'
p_start_date = '2021-01-01'
p_end_date = '2021-03-01'

# sql к функции DELTA_PART
DELTA_PART_SQL = (
    f"select {DB_SCHEMA}.{DB_DELTA_PART}(%(tabname)s,%(tabname_p)s,'{login_p}','{pass_p}',%(tabname)s,%(pk)s,'{p_start_date}'::timestamp, '{p_end_date}'::timestamp);"
)

""" для обновления за вчера
DELTA_PART_SQL = (
    f"select {DB_SCHEMA}.{DB_DELTA_PART}("
    f"%(tabname)s, %(tabname_p)s, '{login_p}', '{pass_p}', %(tabname)s, %(pk)s, "
    f" '{{{{ macros.ds_add(ds, -1) }}}}'::timestamp, '{{{{ ds }}}}'::timestamp);"
)
"""

# для траффика 
traffic_keys = '"date"'
DELTA_PART_SQL_traffic = (
    f"select {DB_SCHEMA}.f_delta_partition_traffic('std12_116_pr.traffic','gp.traffic','{login_p}','{pass_p}','std12_116_pr.traffic', "
    f"'{traffic_keys}','{p_start_date}'::timestamp, '{p_end_date}'::timestamp);"
)

"""
SELECT std12_116_pr.f_delta_partition_traffic(
  'std12_116_pr.traffic',                  -- p_table_from
  'gp.traffic',                         -- pxf-таблица
  'intern',                             -- p_user_id
  'intern',                             -- p_pass
  'std12_116_pr.traffic',                  -- p_table_to
  '"date"',
  '2021-01-01 00:00:00'::timestamp,     -- начало диапазона
  '2021-03-01 00:00:00'::timestamp      -- конец диапазона (не включ.)
);
"""

# для  купонов
p_partition_key = '"date"'
DELTA_PART_SQL_COUPONS = (
    f"select  {DB_SCHEMA}.f_delta_partition_coupons('std12_116_pr.coupons','coupons','std12_116_pr.coupons','{p_partition_key}','{p_start_date}'::timestamp, '{p_end_date}'::timestamp);"
)

""" пример вызова
SELECT std12_116_pr.f_delta_partition_coupons(
  -- источник / staging
  'std12_116_pr.coupons',     -- p_table_from 
  'coupons',            -- p_file_name 
  -- цель / родительская
  'std12_116_pr.coupons',     -- p_table_to 
  '"date"',                   -- p_partition_key
  -- диапазон подгрузки
  '2021-01-01 00:00:00'::timestamp,  -- p_start_date
  '2021-03-01 00:00:00'::timestamp   -- p_end_date (не включительно)
);
"""



#ежедневная витрина
   
DB_LOAD_MART_DAILY = 'f_load_mart_daily'    
LOAD_MART_SQL_DAILY = f"select {DB_SCHEMA}.{DB_LOAD_MART_DAILY}('20210101', '20210301');"

    
#select std12_116_pr.f_load_mart_daily('20210101', '20210301')    


#словарь с настройками DAG
default_args = {
    'depends_on_past': False,                      # Не зависит от предыдущих запусков
    'owner': 'std12_116',                               # Владелец DAG
    'start_date': datetime(2025, 6, 13),            # Дата начала исполнения DAG
    'retries': 1,                                  # Кол-во повторных попыток при ошибке
    'retry_delay': timedelta(minutes=5),           # Задержка между повторами
}

# Создаём сам DAG
with DAG(
    "std12_116_project_dag",                                     # ID DAG
    max_active_runs=3,                              # Максимум одновременно активных DAG
    schedule_interval=None,                         # DAG запускается вручную  schedule_interval='0 0 * * *'
    default_args=default_args,                      # Аргументы по умолчанию
    catchup=False,                                   # Не запускать пропущенные интервалы
    tags=["std12","stepanova"] 
) as dag:
    
    # start
    task_start = DummyOperator(task_id = "start")
    
    # Группируем задачи полной загрузки
    with TaskGroup("full_insert") as task_full_insert_tables:
        for table  in FULL_LOAD_TABLES:
            task = PostgresOperator(
                task_id = f"load_table_{table}",
                postgres_conn_id = DB_CONN,
                sql = MD_TABLE_LOAD_QUERY,
                parameters = {
                    'tabname' : f'{DB_SCHEMA}.{table}',
                    'file_name' : f'{FULL_LOAD_FILES[table]}'
                }                
            )
       
    # Группируем задачи delta_part загрузки
    with TaskGroup("delta_part") as task_DELTA_PART_TABLEs:
        for table  in DELTA_PART_TABLE:
            task = PostgresOperator(
                task_id = f"DELTA_PART_TABLE_{table}",
                postgres_conn_id = DB_CONN,
                sql = DELTA_PART_SQL,
                parameters = {
                    'tabname' : f'{DB_SCHEMA}.{table}',
                    'pk' : f'{DELTA_PART_KEYS[table]}',
                    'tabname_p' : f'gp.{table}'
                }
            )
# Загрузка traffic
        PostgresOperator(
            task_id="DELTA_PART_TABLE_traffic",
            postgres_conn_id=DB_CONN,
            sql=DELTA_PART_SQL_traffic
        )

# Загрузка coupons
        PostgresOperator(
            task_id="DELTA_PART_TABLE_coupons",
            postgres_conn_id=DB_CONN,
            sql=DELTA_PART_SQL_COUPONS
        )


    # витрина
    task_load_mart = PostgresOperator(
        task_id = "load_mart_daily",
        postgres_conn_id = DB_CONN,
        sql = LOAD_MART_SQL_DAILY        
    )
    
    
    # Финальная заглушка
    task_end = DummyOperator(task_id="end")
    
    task_start >>  task_full_insert_tables >> task_DELTA_PART_TABLEs >> task_load_mart >> task_end
