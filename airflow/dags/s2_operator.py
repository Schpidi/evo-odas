from datetime import datetime
from airflow import DAG
from airflow.operators import S2ReaderEOOMOperator


dag = DAG("s2_dag", description="DAG to process Sentinel-2 data",
          schedule_interval="* * * * *",
          start_date=datetime(2017, 5, 4), catchup=False)

generate_eoom = S2ReaderEOOMOperator(
    input_safe_package="/var/data/input/S2A_MSIL1C_20170325T101021_N0204_R022_T32UPU_20170325T101018.SAFE.zip",
    output_directory="/var/data/output/",
    task_id="s2reader_generate_eoom",
    dag=dag
)

generate_eoom
