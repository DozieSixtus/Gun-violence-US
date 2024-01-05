from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.empty import EmptyOperator
from oracle_db_releases import webScraper
from create_connection import create_conn
from report_refresh import trigger_refresh
import re, os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# keyVaultName = "InnovationKey"
# KVUri = f"https://innovationkey.vault.azure.net/"
# power_bi_client_id = 'da20e442-e48c-447c-883e-b0322d2b3d89'

# credential = DefaultAzureCredential()
# key_vault_client = SecretClient(vault_url=KVUri, credential=credential)

# sql_server_secret = key_vault_client.get_secret('DSPAdmin')
# power_bi_client_secret = key_vault_client.get_secret('EmbeddedPowerBI-ServicePrincipal')

power_bi_client_id = 'da20e442-e48c-447c-883e-b0322d2b3d89'

sql_server_secret = os.getenv('SSMS_PASSWORD')
power_bi_client_secret = os.getenv('SP_PASSWORD')

def partition_script(sql_script) -> list:
    """ Function will take the string provided as parameter and cut it on every line that contains only a "GO" string.
        Contents of the script are also checked for commented GO's, these are removed from the comment if found.
        If a GO was left in a multi-line comment, 
        the cutting step would generate invalid code missing a multi-line comment marker in each part.
    :param sql_script: str
    :return: list
    """
    with open(sql_script, 'r') as fd:
        sql_script = fd.read()
    # Regex for finding GO's that are the only entry in a line
    find_go = re.compile(r'^\s*GO\s*$', re.IGNORECASE | re.MULTILINE)
    # Regex to find multi-line comments
    find_comments = re.compile(r'/\*.*?\*/', flags=re.DOTALL)

    # Get a list of multi-line comments that also contain lines with only GO
    go_check = [comment for comment in find_comments.findall(sql_script) if find_go.search(comment)]
    for comment in go_check:
        # Change the 'GO' entry to '-- GO', making it invisible for the cutting step
        sql_script = sql_script.replace(comment, re.sub(find_go, '-- GO', comment))

    # Removing single line comments, uncomment if needed
    # file_content = re.sub(r'--.*$', '', file_content, flags=re.MULTILINE)

    # Returning everything besides empty strings
    return [part for part in find_go.split(sql_script) if part != '']

with DAG(
    dag_id="build_UIM-Report_db",
    schedule_interval="@monthly",
    default_args={
        "owner": "Chiedozie",
        "retries": 1,
        "retry_delay": timedelta(minutes=0.05),
        "start_date": datetime(2023, 11, 29)
    },
    catchup=False
) as dag:  

    create_sql_server_conn = PythonOperator (
        task_id = 'create_sql_server_conn',
        python_callable = create_conn,
        op_kwargs={'conn_id': 'MSSQL_airflow', 'conn_type': 'mssql', 
                   'host': '51.145.80.127', 'login': 'DSPAdmin', 'pwd':f'{sql_server_secret}', 
                   'port': '1433', 'database': 'airflow'},
    )

    oracle_web_scraper = PythonOperator(
        task_id="oracle_web_scraper",
        python_callable = webScraper,
        op_kwargs={'url': 'https://support.oracle.com/knowledge/Oracle%20Database%20Products/742060_1.html',
                   'server': '51.145.80.127', 'username': 'DSPAdmin', 'password':f'{sql_server_secret}', 
                   'database': 'UIM-Report'},
    )

    job_name_1 = job_name_2 = 'testJob'

    sql_job_task_1 = MsSqlOperator(
        task_id='sql_job_1',
        mssql_conn_id='MSSQL_airflow',
        sql=f"EXEC msdb.dbo.sp_start_job N'{job_name_1}';",
        dag=dag,
    )

    sql_job_monitor_1 = SqlSensor(
        task_id='sql_job_monitor_1',
        conn_id='MSSQL_airflow',
        sql=f"""SELECT TOP 1 run_status FROM msdb.dbo.sysjobhistory 
                WHERE job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name_1}') 
                ORDER BY run_date DESC, run_time DESC;""",
        mode='poke',
        poke_interval=10,  # Check every 60 seconds
        timeout=60,  # Timeout after 600 seconds (10 minutes)
        dag=dag,
    )

    sql_job_task_2 = MsSqlOperator(
        task_id='sql_job_2',
        mssql_conn_id='MSSQL_airflow',
        sql=f"EXEC msdb.dbo.sp_start_job N'{job_name_2}';",
        dag=dag,
    )

    sql_job_monitor_2 = SqlSensor(
        task_id='sql_job_monitor_2',
        conn_id='MSSQL_airflow',
        sql=f"""SELECT TOP 1 run_status FROM msdb.dbo.sysjobhistory 
                WHERE job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name_2}') 
                ORDER BY run_date DESC, run_time DESC;""",
        mode='poke',
        poke_interval=10,  # Check every 60 seconds
        timeout=60,  # Timeout after 600 seconds (10 minutes)
        dag=dag,
    )    

    # Example of creating a task that calls an sql command from an external file.
    for i,part in enumerate(partition_script(r"/opt/airflow/dags/SQLQuery1.sql")):
        
        uim_db_task = MsSqlOperator(
            task_id="UIM-Report_query_"+str(i),
            mssql_conn_id="MSSQL_airflow",
            sql=part,
            dag=dag,
        )
        if i == 0:
            uim_db_task.set_upstream(sql_job_monitor_2)
        else:
            uim_db_task.set_upstream(prev_uim_db_task)
        prev_uim_db_task = uim_db_task
        


    powerBI_report_refresh = PythonOperator (
        task_id = 'powerBI_report_refresh',
        python_callable = trigger_refresh,
        op_kwargs={'tenant_name': 'DSP-Explorer.com',
                    'client_id': f'{power_bi_client_id}',
                    'client_secret': f'{power_bi_client_secret}',
                    'workspace_id': '243c79b9-e802-4327-9a0b-c4d57563eaf9', #'f6950485-414e-4ba6-9057-d782d007c2ca'
                    'dataset_id': '8e832455-bc4a-47aa-a08f-ed8980991a05', #'2ad9b2b8-c3a4-4fea-9898-9ad1408eea8b'
                    },
    )

    latest_refresh = MsSqlOperator(
        task_id="latest_refresh",
        mssql_conn_id="MSSQL_airflow",
        sql="""CREATE TABLE demo.LastRefresh (
                SampleDateTime DATETIME
            )
            INSERT INTO demo.LastRefresh VALUES(GETDATE())
            """,
        dag=dag,
    )

sql_job_task_1.set_upstream(create_sql_server_conn)
sql_job_monitor_1.set_upstream(sql_job_task_1)

sql_job_task_2.set_upstream(sql_job_monitor_1)
sql_job_monitor_2.set_upstream(sql_job_task_2)

oracle_web_scraper.set_upstream(sql_job_monitor_2) 

powerBI_report_refresh.set_upstream([uim_db_task, oracle_web_scraper])
latest_refresh.set_upstream(powerBI_report_refresh)
