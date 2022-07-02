import json
from textwrap import dedent
from turtle import down
import pendulum

from airflow import DAG 
import os ,shutil,wget
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col
spark=SparkSession.builder.master('local[*]').appName('fpcc').getOrCreate()


URL_KEY="url"
DOWNLOAD_DIR_KEY="download_dir"
EXTRACT_DIR_KEY="extract_dir"
EXTRACT_FILE_PATH_KEY="extract_file_path"
HDFS_FILE_KEY="hdfs_file"
ZIP_FILE_PATH_KEY="zip_file_path"
FINANCE_CONFIG_INFO_KEY='finance_config_info'


def download_data(url:str,download_dir:str)->str:
    """
    url: download url
    download_dir: download location of file
    
    Return downloaded file path
    """
    from six.moves import urllib
    #if download driectory exists remove it
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
        
    #create download directory
    os.makedirs(download_dir,exist_ok=True)
    file_name=os.path.basename(url).replace(".zip","")
    download_file_path=os.path.join(download_dir,file_name)
    urllib.request.urlretrieve(url, download_file_path)
   
    return download_file_path


def extract_zip_file(zip_file_path:str,extract_file_dir:str)->str:
    """
    zip_file_path: zip file location
    extract_file_dir: Zip file will be extracted at extract file dir
    Returns file_path
    """
    
    #Remove exisiting extract dir
    if os.path.exists(extract_file_dir):
        shutil.rmtree(extract_file_dir)
        
    #Creating extract dir
    os.makedirs(extract_file_dir,exist_ok=True)
    
    
    from zipfile import ZipFile
    with ZipFile(zip_file_path,"r") as zip_file:
        zip_file.extractall(extract_file_dir)
    
    #obtain file name for download directory
    file_name = os.listdir(extract_file_dir)[0]
    
    #preparing file path
    file_path=os.path.join(extract_file_dir,file_name )
    return file_path

def is_hdfs_file_present(file_path:str)->bool:
    jvm = spark._jvm
    jsc = spark._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    return fs.exists(jvm.org.apache.hadoop.fs.Path(file_path))
                     
    
def store_file_in_hdfs(source_file_path:str,hadoop_file_path:str)->None:
    """
    This function updated not exisiting record in hdfs
    
    Parameters:
    source_file_path: json file path in local system
    hadoop_file_path: hadoop file path in HDFS system
    
    Return None
    """
    ingested_date_column_name="ingested_date"
    local_file_start_path_str="file:///"
    if not source_file_path.startswith(local_file_start_path_str):
        if source_file_path.startswith("/"):
            source_file_path=source_file_path[1:]
        source_file_path=f"{local_file_start_path_str}{source_file_path}"
        print(source_file_path)
    #reading json file from disk
    current_complaint_df=spark.read.json(source_file_path)
    print(f"Number of record in downloaded file: {current_complaint_df.select('complaint_id').count()}")
    #Dropping column _corrupt record
    current_complaint_df = current_complaint_df.drop(current_complaint_df._corrupt_record)
    
    #dropping row where complaint id is null
    current_complaint_df=current_complaint_df.where(current_complaint_df.complaint_id.isNotNull())
    #Adding column ingested_date
    current_complaint_df = current_complaint_df.withColumn(ingested_date_column_name,lit(datetime.now()))
    
    
    #if file exists only update not exiting records
    if is_hdfs_file_present(hadoop_file_path):
        #Reading exisiting data from hdfs
        existing_complaint_df = spark.read.parquet(hadoop_file_path)
        print(f"Number of record in already present file: {existing_complaint_df.select('complaint_id').count()}")
        #selecting compaint id which is not available in hdfs
        selected_complaint_id=current_complaint_df.join(
            existing_complaint_df,current_complaint_df.complaint_id==existing_complaint_df.complaint_id,
             how="left").where(existing_complaint_df.complaint_id.isNull()).select(current_complaint_df.complaint_id)
        
        #selecting record to be upadted in hdfs
        current_complaint_df=current_complaint_df.join(selected_complaint_id,
                              current_complaint_df.complaint_id==selected_complaint_id.complaint_id,
                                how="inner").drop(selected_complaint_id.complaint_id)
        #Appedning record in hdfs
        print(f"Number of record will be appened in file: {current_complaint_df.select('complaint_id').count()}")
        current_complaint_df.write.mode('append').parquet(hadoop_file_path)
    else:
        #Creating file as it is not available
        current_complaint_df.write.parquet(hadoop_file_path)
    
    
    

with DAG(
    dag_id='fpcc_etl_hdfs', 
    default_args={'retries':2}, 
    description="This dag responsible to download data from census database and store in hdfs",
    schedule_interval="@daily",
    start_date=datetime(2022,6,2),
    catchup=False, 
    tags=['etl']
    ) as dag:

    def finance_config(**kwargs):
        finance_config_data=kwargs['ti']
        url="https://files.consumerfinance.gov/ccdb/complaints.json.zip"
        download_dir="/root/ipynb/fpcc/finance/data/zip"
        extract_dir="/root/ipynb/fpcc/finance/data/json"
        hdfs_file="/user/root/ineuron/ml/finance-complaint/ingested_data/finance.parquet"
        
       
        config={URL_KEY:url}
        config[DOWNLOAD_DIR_KEY]=download_dir
        config[EXTRACT_DIR_KEY]=extract_dir
        config[HDFS_FILE_KEY]=hdfs_file
        
        finance_config_data.xcom_push(FINANCE_CONFIG_INFO_KEY,config)

    def download(**kwargs):
        finance_config_data=kwargs['ti']
        config=finance_config_data.xcom_pull(task_ids='finance_config',key=FINANCE_CONFIG_INFO_KEY)
        

        url=config[URL_KEY]
        download_dir=config[DOWNLOAD_DIR_KEY]
        zip_file_path=download_data(url=url,download_dir=download_dir)
        config[ZIP_FILE_PATH_KEY]=zip_file_path

        finance_config_data.xcom_push(FINANCE_CONFIG_INFO_KEY,config)

    def extract(**kwargs):
        finance_config_data=kwargs['ti']
        config=finance_config_data.xcom_pull(task_ids='download',key=FINANCE_CONFIG_INFO_KEY)
    
        extract_dir=config[EXTRACT_DIR_KEY]
        zip_file_path= config[ZIP_FILE_PATH_KEY]
        extract_file_path=extract_zip_file(zip_file_path=zip_file_path,
        extract_file_dir=extract_dir
        )
        config[EXTRACT_FILE_PATH_KEY]=extract_file_path
        finance_config_data.xcom_push(FINANCE_CONFIG_INFO_KEY,config)
    
    def store_record(**kwargs):
        finance_config_data=kwargs['ti']
        config=finance_config_data.xcom_pull(task_ids='extract',key=FINANCE_CONFIG_INFO_KEY)
      
        extract_file_path=config[EXTRACT_FILE_PATH_KEY]
        hdfs_file_path=config[HDFS_FILE_KEY]
        store_file_in_hdfs(source_file_path=extract_file_path,hadoop_file_path=hdfs_file_path)

    config_task=PythonOperator(
        task_id='finance_config',
        python_callable=finance_config
    )

    download_task=PythonOperator(

        task_id='download',
        python_callable=download
    )
    extract_task=PythonOperator(
        task_id='extract',
        python_callable=extract
    )
    store_task=PythonOperator(
        task_id='store_record', 
        python_callable=store_record
    )

    config_task >> download_task >> extract_task >> store_task


