import os
import json
from datetime import datetime, timezone, timedelta
from airflow.operators import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow import models
from airflow.contrib.operators.dataproc_operator import DataProcSparkOperator, \
    DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcPySparkOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.helpers import chain
from airflow.operators import bash_operator
from airflow.models import Variable
from base64 import b64encode
from airflow.contrib.operators.pubsub_operator  import PubSubPublishOperator
from airflow.utils.dates import days_ago     

yesterday = datetime.combine(
    datetime.today() - timedelta(1),
    datetime.min.time())

CLUSTER_NAME_PIV_NEC = 'gnp-gdp-autos-piv-nec-{{ds_nodash}}'
CLUSTER_NAME_NEC1 = 'gnp-gdp-autos-nec-{{ds_nodash}}'
CLUSTER_NAME_COB_TP = 'gnp-gdp-autos-cob-tp-{{ds_nodash}}'
CLUSTER_NAME_OBJ_TP = 'gnp-gdp-autos-obj-tp-{{ds_nodash}}'
CLUSTER_NAME_POL_TP = 'gnp-gdp-autos-pol-tp-{{ds_nodash}}'
CLUSTER_NAME_POLIZA = 'gnp-gdp-autos-poliza-{{ds_nodash}}'
CLUSTER_NAME_PIV_ECO = 'gnp-gdp-autos-piv-eco-{{ds_nodash}}'
CLUSTER_NAME_ECO1 = 'gnp-gdp-autos-eco-{{ds_nodash}}'
CLUSTER_NAME_PIV_SIN = 'gnp-gdp-autos-piv-sin-{{ds_nodash}}'
CLUSTER_NAME_SIN1 = 'gnp-gdp-autos-sin-{{ds_nodash}}'

dag_config = Variable.get('GNP-GDP-ConfigVars', deserialize_json=True)

PROJECT_ID = dag_config['project_id']
ENV = PROJECT_ID[8:]#Environment
REGION = dag_config['region']
ZONE = dag_config['zone']
NETWORK = dag_config['network']
PROJECT_NETWORK = dag_config["project_network"]
MASTER_MACHINE = dag_config['master_machine_type']
NUM_MASTERS = dag_config['num_masters']
WORKER_MACHINE = dag_config['worker_machine_type']
AIRFLOW_BUCKET = dag_config['airflow_bucket']
IDLE_DELETE = dag_config['idle_delete']
METADATA_HIVE = {'hive-metastore-instance' : ''+PROJECT_ID+':us-central1:hive-metastore'}
TIPO_CARGA = dag_config['carga'] #'full'
TIPO_CARGA_SINIESTROS = 'full'
DIAS_DELTA = dag_config['diasDelta'] #'1'
REPARIR_T  = 'NONE' #repair = procesa las tablas T para actualizarlas | #NONE no lo ejecuta y solo valida agrega delta a la Tabla C 
MAIL_LIST = dag_config['mailList']
SENDER = dag_config['userSender']
MAIL_PASS = dag_config['pass']
TOPIC_GCP_PROJECT_ID = ''+PROJECT_ID+''
TOPIC = 'arrebol-model-start-process'
TODAYF= datetime.now().strftime('%Y%m%d')
DAY_REVAL = dag_config['execReval']


def CREATE_CLUSTER_TASK(task_id, cluster_name, num_workers):
    task = DataprocClusterCreateOperator(
        task_id=task_id,
        cluster_name=cluster_name,
        project_id=PROJECT_ID,
        num_masters=NUM_MASTERS,
        num_workers=num_workers,
        master_machine_type = MASTER_MACHINE,
        master_disk_size=1024,
        worker_machine_type = WORKER_MACHINE,
        worker_disk_size=1024,
        image_version = "1.4",
        tags=['dataproc'],
        idle_delete_ttl=IDLE_DELETE,
        properties = {"hive:hive.metastore.warehouse.dir" : "gs://gnp-dlk-pro-warehouse/datasets"},
        metadata = METADATA_HIVE,
        region =REGION,
        zone=ZONE,
        subnetwork_uri='projects/'+PROJECT_NETWORK+'/regions/us-central1/subnetworks/'+NETWORK,
        service_account=PROJECT_ID+'@appspot.gserviceaccount.com',
        service_account_scopes = ['https://www.googleapis.com/auth/cloud-platform']
    )
    return task

def get_day():
    d = datetime.now()
    if d.strftime('%d') == DAY_REVAL:
        return 'rvl_salv_pe_paso1'
    else:
        return 'NOT_EXEC_REVAL'


def get_load_type():
    if TIPO_CARGA == "delta" :
        return "NOT_EXEC_CIFRAS"
    else:
        return "people_cc_ban_gen_par_0_py"


default_dag_args = {
    'owner': 'AUTOS',
    'start_date': datetime(2021, 4, 9),
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with models.DAG(
        'GNP-GDP-AUTOS',
        schedule_interval='0 7 * * *', # 7 Horario de Verano
        #schedule_interval='0 8 * * *', # 8 para horario normal
        max_active_runs=1,
        #schedule_interval=None,
        default_args=default_dag_args) as dag:

    CREATE_CLUSTER_PIV_NEC = CREATE_CLUSTER_TASK('CLUSTER_PIV_NEC',CLUSTER_NAME_PIV_NEC,50)

    CREATE_CLUSTER_NEC1 = CREATE_CLUSTER_TASK('CREATE_CLUSTER_NEC1',CLUSTER_NAME_NEC1,20)
    CREATE_CLUSTER_COB_TP = CREATE_CLUSTER_TASK('CREATE_CLUSTER_COB_TP',CLUSTER_NAME_COB_TP,45)
    CREATE_CLUSTER_OBJ_TP = CREATE_CLUSTER_TASK('CREATE_CLUSTER_OBJ_TP',CLUSTER_NAME_OBJ_TP,40)
    CREATE_CLUSTER_POL_TP = CREATE_CLUSTER_TASK('CREATE_CLUSTER_POL_TP',CLUSTER_NAME_POL_TP,25)
    CREATE_CLUSTER_POLIZA = CREATE_CLUSTER_TASK('CREATE_CLUSTER_POLIZA',CLUSTER_NAME_POLIZA,40)#40
    
    CREATE_CLUSTER_ECO1 = CREATE_CLUSTER_TASK('CREATE_CLUSTER_ECO1',CLUSTER_NAME_ECO1,40)
    CREATE_CLUSTER_NEC2 = CREATE_CLUSTER_TASK('CREATE_CLUSTER_NEC2',CLUSTER_NAME_NEC1,50)#60
    CREATE_CLUSTER_ECO2 = CREATE_CLUSTER_TASK('CREATE_CLUSTER_ECO2',CLUSTER_NAME_ECO1,50)#80

    DELETE_HIVE_DATASETS = bash_operator.BashOperator(
        task_id='DELETE_HIVE_DATASETS',
        bash_command='gsutil rm -r gs://'+PROJECT_ID+'-warehouse/datasets'
    )
    
    CARGA_SEMANTICA_ECO_1 = bash_operator.BashOperator(
        task_id="BQ_UPLOAD_EMISION_COBERTURA",
        bash_command="bq load --replace --source_format=AVRO --use_avro_logical_types=TRUE semantica.AUT_INSUMOS_SB1_EMISION_COBERTURA gs://gnp-dlk-pro-analitica/autos/emision/AUT_INSUMOS_SB1_EMISION_COBERTURA/data/*.avro "
    )
    
    CARGA_SEMANTICA_ECO_2 = bash_operator.BashOperator(
        task_id="BQ_UPLOAD_EMISION_POLIZA",
        bash_command="bq load --replace --source_format=AVRO --use_avro_logical_types=TRUE semantica.AUT_INSUMOS_SB1_EMISION_POLIZA gs://gnp-dlk-pro-analitica/autos/emision/AUT_INSUMOS_SB1_EMISION_POLIZA/data/*.avro "
    )

    CARGA_SEMANTICA_SIN_1 = bash_operator.BashOperator(
        task_id="BQ_UPLOAD_COB_AFECTA",
        bash_command="bq load --replace --use_avro_logical_types=TRUE --source_format=AVRO semantica.AUT_SINIESTRO_COB_AFECTA gs://gnp-dlk-pro-analitica/autos/siniestros/AUT_SINIESTRO_COB_AFECTA_V2/data/*.avro "
    )
    
    CARGA_SEMANTICA_SIN_2 = bash_operator.BashOperator(
        task_id="BQ_UPLOAD_SINIESTRO_POLIZA",
        bash_command="bq load --replace --use_avro_logical_types=TRUE --source_format=AVRO semantica.AUT_SINIESTRO_POLIZA gs://gnp-dlk-pro-analitica/autos/siniestros/AUT_SINIESTRO_POLIZA/data/*.avro "
    )

    SCALE_COB_DC = bash_operator.BashOperator(
        task_id='SCALE_COBERTURAS',
        bash_command='gcloud dataproc clusters update '+CLUSTER_NAME_COB_TP+' --num-workers 60 --region us-central1 '
    )
    
    CARGA_SEMANTICA_SINIESTROS_POLIZA_BRECHAS = bash_operator.BashOperator(
        task_id="BQ_UPLOAD_AUT_SINIESTRO_POLIZA_BRECHAS",
        bash_command="bq load --replace --source_format=AVRO --use_avro_logical_types=TRUE semantica.AUT_SINIESTRO_POLIZA_BRECHAS gs://gnp-dlk-pro-modelado/autos/siniestros/transformaciones/AUT_SINIESTRO_POLIZA_B/data/*.avro "
    )
    
    CARGA_SEMANTICA_SINIESTROS_COB_BRECHAS = bash_operator.BashOperator(
        task_id="BQ_UPLOAD_AUT_SINIESTRO_COB_BRECHAS",
        bash_command="bq load --replace --source_format=AVRO --use_avro_logical_types=TRUE semantica.AUT_SINIESTRO_COB_AFECTA_BRECHAS gs://gnp-dlk-pro-modelado/autos/siniestros/transformaciones/AUT_SINIESTRO_COB_AFECTA_B/data/*.avro "
    )
    
    DELETE_ECO_DATA = bash_operator.BashOperator(
        task_id='GCP_DELETE_ECO_DATA',
        bash_command='gsutil rm -r gs://gnp-dlk-pro-crudos/info/TPIVOTE_ECO/data '
    )
    DELETE_ECO2_DATA = bash_operator.BashOperator(
        task_id='GCP_DELETE_ECO2_DATA',
        bash_command='gsutil rm -r gs://gnp-dlk-pro-crudos/info/TPIVOTE_ECO2/data '
    )

    DELAY_NEC = bash_operator.BashOperator(
        task_id='GCP_CP_PIV_ECO',
        bash_command='gsutil cp -r gs://gnp-dlk-'+ENV+'-crudos/info/TPIVOTE/data gs://gnp-dlk-'+ENV+'-crudos/info/TPIVOTE_ECO/ '
    )

    COPY_PIV_ECO2 = bash_operator.BashOperator(
        task_id='GCP_CP_PIV_ECO2',
        bash_command='gsutil cp -r gs://gnp-dlk-'+ENV+'-crudos/info/TPIVOTE/data gs://gnp-dlk-'+ENV+'-crudos/info/TPIVOTE_ECO2/ '
    )

    WAIT_TO_DELETE_NEC = bash_operator.BashOperator(
        task_id='WAIT_TO_DELETE_NEC',
        bash_command='sleep 120s '
    )

    DELAY_POLIZA = bash_operator.BashOperator(
        task_id='DELAY_POLIZA',
        bash_command='sleep 180s '
    )
    
    WAIT_TO_DELETE_ECO = bash_operator.BashOperator(
        task_id='WAIT_TO_DELETE_ECO',
        bash_command='sleep 180s ',
        trigger_rule="all_done"
    )
    
    REMOVE_PREV_EMI_COB_BK = bash_operator.BashOperator(
        task_id="REMOVE_PREV_EMI_COB_BK",
        bash_command="gsutil -m rm gs://gnp-dlk-pro-analitica/autos/emision/AUT_INSUMOS_SB1_EMISION_COBERTURA/data_bk/* "
    )
    
    BACKUP_EMI_COB = bash_operator.BashOperator(
        task_id="BACKUP_EMI_COB",
        bash_command="gsutil -m cp gs://gnp-dlk-pro-analitica/autos/emision/AUT_INSUMOS_SB1_EMISION_COBERTURA/data/*  gs://gnp-dlk-pro-analitica/autos/emision/AUT_INSUMOS_SB1_EMISION_COBERTURA/data_bk  "
    )

    REMOVE_PREV_EMI_POL_BK = bash_operator.BashOperator(
        task_id="REMOVE_PREV_EMI_POL_BK",
        bash_command="gsutil -m rm gs://gnp-dlk-pro-analitica/autos/emision/AUT_INSUMOS_SB1_EMISION_POLIZA/data_bk/* "
    )

    BACKUP_EMI_POL = bash_operator.BashOperator(
        task_id="BACKUP_EMI_POL",
        bash_command="gsutil -m cp gs://gnp-dlk-pro-analitica/autos/emision/AUT_INSUMOS_SB1_EMISION_POLIZA/data/*  gs://gnp-dlk-pro-analitica/autos/emision/AUT_INSUMOS_SB1_EMISION_POLIZA/data_bk  "
    )
    
    #Declaracion artefactos Tablas pivote ----------------------
    TPIVOTE_NO_ECO = DataProcSparkOperator(
        task_id = 'TPIVOTE_MASTER',
        region = REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-Tpivote_NEC_2.jar'],
        main_class = 'com.gnp.mx.gcp_pivote.App',
        cluster_name = CLUSTER_NAME_PIV_NEC,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA,DIAS_DELTA,ENV]
    )
    #-------------------------------------------------------
        
    # NO ENCONOMICO
    AUT_CONTRATO = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-Contrato',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-Contrato.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.ContratoApp',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['contrato.yaml', TIPO_CARGA]
    )

    AUT_FOLIO_AGENTE = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-FolioAgente',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-FolioAgente.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.EstandarizaGFVTFLA0App',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_PORC_COMISION_REF_INTERMEDIARIO_MCT = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-ComisionRefIntermediarioMCT',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-ComisionRefIntermediarioMCT.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.EstandarizaKCIM40App',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_DATOS_CONTACTO = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-DatosContacto',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-DatosContacto.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.AUT_DATOS_CONTACTO',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_POLIZARIO = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-Polizario',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-Polizario.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.AutPolizarioApp',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    OBJETOS_DC = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-ObjetosDC',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-ObjetosDC.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.ObjetosDC',
        cluster_name = CLUSTER_NAME_OBJ_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4', 'spark.driver.extraJavaOptions' : '-Xss512m' ,'driver-java-options':'-Xss512m'},
        arguments = [ENV,'autos/emision/no_economico/config/parametros_obj.txt','info','crudos','KCIM18','KTPA82C','autos','modelado','emision','no_economico','AUT_ELEMENTO_OBJETO',TIPO_CARGA,'0']
    )
    
    AUT_ELEMENTO_OBJETO_TP = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-ElementoObjetoTP',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-ElementoObjetoTP.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.Aut_Elemento_Objeto_TP',
        cluster_name = CLUSTER_NAME_OBJ_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    COBERTURA_DC = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-CoberturaDC',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-CoberturaDC.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.CoberturaDC',
        cluster_name = CLUSTER_NAME_COB_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta,org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [ENV,'autos/emision/no_economico/config/parametros_cob.txt','info','crudos','KCIM23','KTPA82C','autos','modelado','emision','no_economico','AUT_ELEMENTO_COBERTURA',TIPO_CARGA,'0']
    )
    
    COB_COMPLETA_TFUENTE = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-CobCompletasFuente',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-CobCompletasFuente.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.CobCompletaTFuenteApp',
        cluster_name = CLUSTER_NAME_COB_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta,org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    COB_COMPLETAS3_PART2_HIVE = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-CobCompletasElemPart2_HIVE',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-CobCompletasElemPart2.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.CobCompletaPart2',
        cluster_name = CLUSTER_NAME_COB_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta,org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    COB_COMPLETAS3_ELEM = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-CobCompletasElem',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-CobCompletasElem.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.CobCompletaElem',
        cluster_name = CLUSTER_NAME_COB_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta,org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_ELEMENTO_COBERTURA_TP = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-ElementoCoberturaTP',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-ElementoCoberturaTP.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.Aut_Elemento_Cobertura_TP',
        cluster_name = CLUSTER_NAME_COB_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta,org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    POLIZAS_DC = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-PolizasDC',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-PolizasDC.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.PolizasDC',
        cluster_name = CLUSTER_NAME_POL_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta,org.apache.spark:spark-avro_2.11:2.4.4','spark.driver.extraJavaOptions' : '-Xss512m' ,'driver-java-options':'-Xss512m'},
        arguments = [ENV,'autos/emision/no_economico/parametros_pol.txt','info','crudos','KCIM16','KTPA82C','autos','modelado','emision','no_economico','AUT_ELEMENTO_POLIZADC',TIPO_CARGA,'1']
    )

    AUT_ELEMENTO_POLIZA_TP = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-ElementoPolizaTP',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-ElementoPolizaTP.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.Aut_Elemento_Poliza_TP',
        cluster_name = CLUSTER_NAME_POL_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_REL_AGENTE_ENDOSO = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-RelAgenteEndoso',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-RelAgenteEndoso.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.AutRelacionAgenteEndoso',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_DATOS_CONTRATANTE = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-DatosContratante',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-DatosContratante.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.AutDatosContratanteApp',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_DATOS_PAGADOR = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-DatosPagador',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-DatosPagador.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.DatosPagador',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_DATOS_ASEGURADO = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-DatosAsegurado',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-DatosAsegurado.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.AutDatosAsegurado',
        cluster_name = CLUSTER_NAME_OBJ_TP,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_POLIZA = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-Poliza',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/GNP-GDP-AUT-Poliza.jar'],
        main_class = 'com.gnp.mx.insumos_autos_poliza.App',
        cluster_name = CLUSTER_NAME_POLIZA,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4', 'spark.executor.cores':'5'},
        arguments = [TIPO_CARGA]
    )

    AUT_COASEGURO_POLIZA = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT--Coaseguro-Poliza',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-CoaseguroPoliza.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.AutCoaseguroPolizaApp',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4', 'spark.executor.cores':'5'},
        arguments = [TIPO_CARGA]
    )

    AGENTE_NIVEL_POLIZA_VERSION = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-PorcAgenteNivelPolizaVersion',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-PorcAgenteNivelPolizaVersion.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.AutPorcAgenteNivelPolizaVersion',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    APP0 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-AgenteProductor_0',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-AgenteProductor.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.app0',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    APP1 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-AgenteProductor_1',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-AgenteProductor.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.app1',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    APP2 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-AgenteProductor_2',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-AgenteProductor.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.app2',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    APP3 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-AgenteProductor_3',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-AgenteProductor.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.app3',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    APP4 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-AgenteProductor_4',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-AgenteProductor.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.app4',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    APP5 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-AgenteProductor_5',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-AgenteProductor.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.app5',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    APP6 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-AgenteProductor_6',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-AgenteProductor.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.app6',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AGENTE_SUPERVISOR = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-AgenteSupervisor',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-AgenteSupervisor.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.AutAgenteSupervisorSpark',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    UDI_PRIMARIA = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-UDIPrimaria',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-UDIPrimaria.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.agentes.AgenteUDIPrimaria',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta,org.apache.spark:spark-avro_2.11:2.4.4' },
        arguments = [TIPO_CARGA,ENV]
    )
    
    UDI_SECUNDARIA = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-UDISecundaria',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/agentes/GNP-GDP-AUT-UDISecundaria.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.AUT_AGENTE_UDISECUNDARIA',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA,ENV]
    )
    # SEMANTICA NO ECONOMICOS

    AUT_POLIZA_PAQUETE = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-PolizaPaquete',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-PolizaPaquete.jar'],
        main_class = 'mx.com.gnp.datalake.AutPolizaPaqueteApp',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_SEMANTICA_NEC_P01 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso1',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso1.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso1',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
     
    AUT_SEMANTICA_NEC_P02 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso2',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso2.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso2',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P03 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso3',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso3.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso3',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P04 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso4',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso4.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso4',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P05 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso5',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso5.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso5',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P06 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso6',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso6.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso6',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P07 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso7',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso7.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso7',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P08 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso8',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso8.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso8',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P09 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso9',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso9.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso9',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P10 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso10',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso10.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso10',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P11 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso11',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso11.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso11',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )
    
    AUT_SEMANTICA_NEC_P12 = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaNoEcoPaso12',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/semantica/GNP-GDP-AUT-SemanticaNoEcoPaso12.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.noeconomico.semantica.SemanticaPaso12',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    # ECONOMICO
    ####  SecciÃ³n malla EconÃ³mica
    AUT_COASEGURO = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-Coaseguro',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-Coaseguro.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.AutCoaseguroApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_COMISION = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-Comision',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-Comision.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.AutComisionApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_COMISION_SERVICIOS_CONEXO = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-ComisionServiciosConexos',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-ComisionServiciosConexos.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.ComisionServiciosConexosApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_SERVICIO_CONEXO = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-ServicioConexo',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-ServicioConexo.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.AutServicioConexoApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_UDI = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-UDI',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-UDI.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.AutUDIImpalaApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_UDIS_CONEXOS = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-UDIsConexos',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-UDIsConexos.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.AutUDISConexosApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    AUT_PRIMAS = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-Primas',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-Primas.jar'],
        main_class = 'mx.com.gnp.datalake.AutPrimasApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA, ENV]
    )

    COASEGURO = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-CoaseguroCia',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-CoaseguroCia.jar'],
        main_class = 'mx.com.gnp.datalake.CoaseguroApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    COMISION_CEDIDA = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-ComisionCedida',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/GNP-GDP-AUT-ComisionCedida.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.Parte1Eco8ComisionCedidaApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_1_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso1',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso1.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.app',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_2_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso2',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso2.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.app',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_3_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso3',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso3.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.app',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_4_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso4',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso4.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.app',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_5_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso5',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso5.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.app',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_6_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso6',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso6.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.app',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_7_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso7',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso7.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.app',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_8_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso8',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso8.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.app',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_9_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso9',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso9.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.semantica.AutSemanticaEcoP09',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['full', TIPO_CARGA]
    )

    PASO_10_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso10',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso10.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.Paso10EconomicoLIBIIApp',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_11_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso11',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso11.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.Paso_11_Economico_LIBII',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['full', TIPO_CARGA]
    )

    PASO_12_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso12',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso12.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.Paso_12_Economico_LIBII',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_13_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso13',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso13.jar'],
        main_class = 'mx.com.gnp.datalake.autos.economico.paso_13_economico_libii.App',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_14_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso14',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso14.jar'],
        main_class = 'mx.com.gnp.datalake.autos.economico.paso_14_economico_libii.App',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_15_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso15',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso15.jar'],
        main_class = 'mx.com.gnp.datalake.autos.economico.paso_15_economico_libii.App',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_16_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso16',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso16.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.Paso_16_Economico_LIBII',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_17_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso17',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso17.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.Paso_17_Economico_LIBII',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_18_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso18',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso18.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.Paso_18_Economico_LIBII',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_19_LIBII = DataProcSparkOperator(
        task_id = 'AUT_INSUMOS_SB1_EMISION_COBERTURA_P19',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso19.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.Paso_19_Economico_LIBII',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_20_22_LIBII = DataProcSparkOperator(
        task_id = 'GNP-GDP-AUT-SemanticaEcoPaso20_22',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso20_22.jar'],
        main_class = 'mx.com.gnp.datalake.gcp.autos.emision.economico.Paso_20_22_Economico_LIBII',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    PASO_23_25_LIBII = DataProcSparkOperator(
        task_id = 'AUT_INSUMOS_SB1_EMISION_POLIZA_P23_25',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/emision/economico/semantica/GNP-GDP-AUT-SemanticaEcoPaso23_25.jar'],
        main_class = 'com.gnp.mx.insumos.autos.economicos.App',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA]
    )

    CC_EMISION_PS_1 = DataProcPySparkOperator(
        task_id = 'people_cc_ban_gen_par_0_py',
        region =REGION,
        main = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/jobs/people_cc_ban_gen_par_0.py',
        pyfiles = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/packages.zip',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_pyspark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['--ruta_config_hdfs', 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/configs/paramCifControl.json', '--env', 'gcp', '--fini', '2013-10-01', '--ffin', '2020-07-01']
    )

    CC_EMISION_PS_2 = DataProcPySparkOperator(
        task_id = 'emision_people_cc_ban_gen_par_1_py',
        region =REGION,
        main = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/jobs/emision_people_cc_ban_gen_par_1.py',
        pyfiles = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/packages.zip',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_pyspark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['--ruta_config_hdfs', 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/configs/paramCifControl.json', '--env', 'gcp', '--fini', '2013-10-01', '--ffin', '2020-07-01']
    )
    CC_EMISION_PS_3 = DataProcPySparkOperator(
        task_id = 'emision_cc_pametros_py',
        region =REGION,
        main = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/jobs/emision_cc_pametros.py',
        pyfiles = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/packages.zip',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_pyspark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['--ruta_config_hdfs', 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/configs/paramCifControl.json', '--env', 'gcp', '--fini', '2013-10-01', '--ffin', '2020-07-01']
    )
    CC_EMISION_PS_4 = DataProcPySparkOperator(
        task_id = 'emision_people_cc_ban_gen_par_2_py',
        region =REGION,
        main = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/jobs/emision_people_cc_ban_gen_par_2.py',
        pyfiles = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/packages.zip',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_pyspark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['--ruta_config_hdfs', 'gs://'+PROJECT_ID+'-workspaces/autos/emision/cifras_control/GNP-GDP-AUT-EmisionCifrasControl/configs/paramCifControl.json', '--env', 'gcp', '--fini', '2013-10-01', '--ffin', '2020-07-01']
    )

    EMISION_BRECHAS_COBERTURA = DataProcPySparkOperator(
        task_id = 'EMISION_BRECHAS_COBERTURA',
        region =REGION,
        main = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/jobs/brechas_emision_cobertura_ajuste_vers_1p0.py',
        pyfiles = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/packages.zip',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_pyspark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['--ruta_config', 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/configs/brechas_emision_cobertura_config.json']
    )

    EMISION_BRECHAS_REMEDY_COB = DataProcPySparkOperator(
        task_id = 'EMISION_BRECHAS_REMEDY_COB',
        region =REGION,
        main = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/jobs/brechas_emision_cobertura_ajuste_remedy.py',
        pyfiles = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/packages.zip',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_pyspark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['--ruta_config', 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/configs/brechas_emision_cobertura_remedy_config.json']
    )

    EMISION_BRECHAS_POLIZA = DataProcPySparkOperator(
        task_id = 'EMISION_BRECHAS_POLIZA',
        region =REGION,
        main = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/jobs/brechas_emision_poliza_ajuste_vers_1p0.py',
        pyfiles = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/packages.zip',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_pyspark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['--ruta_config', 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/configs/brechas_emision_poliza_config.json']
    )

    EMISION_BRECHAS_REMEDY_POL = DataProcPySparkOperator(
        task_id = 'EMISION_BRECHAS_REMEDY_POL',
        region =REGION,
        main = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/jobs/brechas_emision_poliza_ajuste_remedy.py',
        pyfiles = 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/packages.zip',
        cluster_name = CLUSTER_NAME_ECO1,
        dataproc_pyspark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = ['--ruta_config', 'gs://'+PROJECT_ID+'-workspaces/autos/emision/transf_semantica/GNP-GDP-AUT-EmisionBrechas/configs/brechas_emision_poliza_remedy_config.json']
    )

    #####################################################################################################
    #CIFRAS CONTROL
    CIFRAS_CONTROL_MODELADO = DataProcSparkOperator(
        task_id = 'GNP_GDP_AUT_CIFRAS_MODELADO',
        region =REGION,
        dataproc_spark_jars = ['gs://'+PROJECT_ID+'-workspaces/autos/cifras_control/GNP_GDP_AUT_CIFRAS.jar','gs://'+PROJECT_ID+'-workspaces/autos/cifras_control/lib/javax.mail-api-1.6.2-sources.jar','gs://'+PROJECT_ID+'-workspaces/autos/cifras_control/lib/mail-1.5.0-b01.jar'],
        main_class = 'com.gnp.mx.insumos.autos.control.App',
        cluster_name = CLUSTER_NAME_NEC1,
        dataproc_spark_properties = {'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments = [TIPO_CARGA, 'modelado', MAIL_LIST, SENDER, MAIL_PASS]
    )
  #####################################################################################################        

    DELETE_CLUSTER_PIV_NEC = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_PIV_NEC',
        cluster_name=CLUSTER_NAME_PIV_NEC,
        region = REGION,
        project_id=''+PROJECT_ID+''
    )

    DELETE_CLUSTER_NEC1 = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_NEC1',
        cluster_name=CLUSTER_NAME_NEC1,
        region = REGION,
        project_id=''+PROJECT_ID+''
    )

    DELETE_CLUSTER_COB_TP = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_COB_TP',
        cluster_name=CLUSTER_NAME_COB_TP,
        region = REGION,
        project_id=''+PROJECT_ID+''
    )

    DELETE_CLUSTER_OBJ_TP = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_OBJ_TP',
        cluster_name=CLUSTER_NAME_OBJ_TP,
        region = REGION,
        project_id=''+PROJECT_ID+''
    )

    DELETE_CLUSTER_POL_TP = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_POL_TP',
        cluster_name=CLUSTER_NAME_POL_TP,
        region = REGION,
        project_id=''+PROJECT_ID+''
    )

    DELETE_CLUSTER_POLIZA = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_POLIZA',
        cluster_name=CLUSTER_NAME_POLIZA,
        region = REGION,
        project_id=''+PROJECT_ID+''
    )

    #DELETE_CLUSTER_PIV_ECO = DataprocClusterDeleteOperator(
    #    task_id='DELETE_CLUSTER_PIV_ECO',
    #    cluster_name=CLUSTER_NAME_PIV_ECO,
    #    region = REGION,
    #    project_id=''+PROJECT_ID+''
    #)

    DELETE_CLUSTER_ECO1 = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_ECO1',
        cluster_name=CLUSTER_NAME_ECO1,
        region = REGION,
        project_id=''+PROJECT_ID+''
    )

    DELETE_CLUSTER_NEC2 = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_NEC2',
        cluster_name=CLUSTER_NAME_NEC1,
        region = REGION,
        project_id=''+PROJECT_ID+''
        
    )

    DELETE_CLUSTER_ECO2 = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_ECO2',
        cluster_name=CLUSTER_NAME_ECO1,
        region = REGION,
        project_id=''+PROJECT_ID+'',
        trigger_rule="all_done"
    )

    AUTOS_INIT = DummyOperator(
        task_id='AUTOS_GCP'
    )

    ELEMENTOS_INIT = DummyOperator(
        task_id='ELEMENTOS'
    )

    AGENTES_INIT = DummyOperator(
        task_id='AGENTES'
    )

    NEC1_END = DummyOperator(
        task_id='FIN_NO_ECONOMICO'
    )

    ECO1_END = DummyOperator(
        task_id='FIN_ECONOMICO'
    )

    AUTOS_END = DummyOperator(
        task_id='FIN_AUTOS_EMISION'
    )
    
    SEMANTICA_EMI_INIT = DummyOperator(
        task_id='SEMANTICA_EMISION'
    )

    CIFRAS_PEOPLE_SOFT = DummyOperator(
        task_id='GNP-GDP-AUT-EmisionCifrasControl_PS'
    )

    PRODUCTOS = DummyOperator(
        task_id='Productos'
    )

    def modelPubSubMessage(t): 
        return PubSubPublishOperator (
            task_id='ModelMessage-{t}'.format(t=t),
            project=TOPIC_GCP_PROJECT_ID,
            topic=TOPIC,
            messages= [{'data': str(b64encode(json.dumps({'table':t,'tipo_carga': TIPO_CARGA, 'date': TODAYF}).encode('utf-8')), 'utf-8'),'attributes': {'Content-Type': 'application/json'}}],
            create_topic=True,
            dag=dag,
        )
        
    def sleepMinute(id, minutes):
        return bash_operator.BashOperator(
            task_id='Sleep-{minutes}-{id}'.format(minutes=minutes,id=id),
            bash_command='sleep {minutes}m '.format(minutes=minutes),
            dag=dag
        )    

    def cobElemPart2(prodTec):
        return DataProcPySparkOperator (
            task_id=str(prodTec).replace('.',''),
            region=REGION,
            main='gs://'+PROJECT_ID+'-workspaces/autos/emision/no_economico/cobCompletasPart2_'+TIPO_CARGA+'_optimized.py',
            cluster_name=CLUSTER_NAME_COB_TP,
            dataproc_pyspark_properties={'spark.jars.packages' : 'org.apache.spark:spark-avro_2.11:2.4.4'},
            arguments=[PROJECT_ID,str(prodTec).replace('.','')],dag=dag)

    NOT_EXEC_CIFRAS = DummyOperator(task_id='NOT_EXEC_CIFRAS')

    branch_cifras = BranchPythonOperator(
        task_id='BRANCH_CIFRAS',
        python_callable=get_load_type,
        trigger_rule="all_done"
    )

    ############## INICIO GENERACION PIVOTES ###############################
    AUTOS_INIT.set_downstream(CREATE_CLUSTER_PIV_NEC)
    CREATE_CLUSTER_PIV_NEC.set_downstream(TPIVOTE_NO_ECO)
    TPIVOTE_NO_ECO.set_downstream([DELETE_ECO_DATA,DELETE_ECO2_DATA])
    DELETE_ECO_DATA >> DELAY_NEC
    DELETE_ECO2_DATA >> COPY_PIV_ECO2
    DELAY_NEC >> DELETE_CLUSTER_PIV_NEC
    COPY_PIV_ECO2 >> DELETE_CLUSTER_PIV_NEC

    DELETE_CLUSTER_PIV_NEC.set_downstream([CREATE_CLUSTER_NEC1, CREATE_CLUSTER_ECO1])
    CREATE_CLUSTER_NEC1.set_downstream([AUT_PORC_COMISION_REF_INTERMEDIARIO_MCT,AUT_FOLIO_AGENTE,AUT_DATOS_CONTACTO,\
    AUT_POLIZARIO,AUT_CONTRATO,ELEMENTOS_INIT,DELETE_HIVE_DATASETS])

    AUT_PORC_COMISION_REF_INTERMEDIARIO_MCT >> modelPubSubMessage('AUT_PORC_COMISION_REF_INTERMEDIARIO_MCT')
    AUT_FOLIO_AGENTE >> modelPubSubMessage('AUT_FOLIO_AGENTE')
    AUT_DATOS_CONTACTO >> modelPubSubMessage('AUT_DATOS_CONTACTO')
    AUT_POLIZARIO >> modelPubSubMessage('AUT_POLIZARIO')
    AUT_CONTRATO >> modelPubSubMessage('AUT_CONTRATO')
    ############## FIN GENERACION PIVOTES ##################################
    
    ############## INICIO MODELADO NO ECONOMICO ########################
    ##############FLUJO ELEMENTOS########################
    ELEMENTOS_INIT.set_downstream([CREATE_CLUSTER_COB_TP,CREATE_CLUSTER_POL_TP,CREATE_CLUSTER_OBJ_TP])
    #############  BLOQUE COBERTURAS_TP
    CREATE_CLUSTER_COB_TP.set_downstream(COBERTURA_DC)
    COBERTURA_DC.set_downstream(COB_COMPLETA_TFUENTE)
    COB_COMPLETA_TFUENTE.set_downstream(COB_COMPLETAS3_PART2_HIVE)

    #COB_COMPLETAS3_PART2_HIVE.set_downstream(COB_COMPLETAS3_PART2)
    COB_COMPLETAS3_PART2_HIVE.set_downstream(PRODUCTOS)

    f = open('/home/airflow/gcs/dags/autos/prod_tecs/config/ProductosTecnicos.txt', 'r')
    for x in f:
        PRODUCTOS >> cobElemPart2(x[:-1]) >> COB_COMPLETAS3_ELEM
    f.close()

    #COB_COMPLETAS3_PART2.set_downstream(COB_COMPLETAS3_ELEM)
    
    COB_COMPLETAS3_ELEM.set_downstream(AUT_ELEMENTO_COBERTURA_TP)
    AUT_ELEMENTO_COBERTURA_TP >> modelPubSubMessage('AUT_ELEMENTO_COBERTURA_TP')
    AUT_ELEMENTO_COBERTURA_TP.set_downstream(DELETE_CLUSTER_COB_TP)
    DELETE_CLUSTER_COB_TP.set_downstream(NEC1_END)
    #############  FIN BLOQUE COBERTURAS_TP
    #############  BLOQUE OBJETOS_TP
    CREATE_CLUSTER_OBJ_TP.set_downstream(OBJETOS_DC)
    OBJETOS_DC.set_downstream(AUT_ELEMENTO_OBJETO_TP)
    AUT_ELEMENTO_OBJETO_TP >> modelPubSubMessage('AUT_ELEMENTO_OBJETO_TP')
    AUT_ELEMENTO_OBJETO_TP.set_downstream(AUT_DATOS_ASEGURADO)
    AUT_DATOS_ASEGURADO >> modelPubSubMessage('AUT_DATOS_ASEGURADO')
    AUT_DATOS_ASEGURADO.set_downstream(DELETE_CLUSTER_OBJ_TP)
    DELETE_CLUSTER_OBJ_TP.set_downstream(DELAY_POLIZA)
    DELAY_POLIZA >> CREATE_CLUSTER_POLIZA 
    #############  FIN BLOQUE OBJETOS_TP
    #############  BLOQUE POLIZAS_TP
    CREATE_CLUSTER_POL_TP.set_downstream(POLIZAS_DC)
    POLIZAS_DC.set_downstream(AUT_ELEMENTO_POLIZA_TP)
    AUT_ELEMENTO_POLIZA_TP >> modelPubSubMessage('AUT_ELEMENTO_POLIZA_TP')
    AUT_ELEMENTO_POLIZA_TP.set_downstream(DELETE_CLUSTER_POL_TP)
    DELETE_CLUSTER_POL_TP.set_downstream(DELAY_POLIZA)
    DELAY_POLIZA >> CREATE_CLUSTER_POLIZA 
    #############  FIN BLOQUE POLIZAS_TP
    ##############FLUJO AGENTES########################
    AUT_POLIZARIO.set_downstream(AGENTES_INIT)
    AUT_POLIZARIO.set_downstream([AUT_ELEMENTO_POLIZA_TP, AUT_ELEMENTO_OBJETO_TP])
    AGENTES_INIT.set_downstream(AUT_REL_AGENTE_ENDOSO)
    AUT_ELEMENTO_OBJETO_TP.set_upstream(OBJETOS_DC)    
    AUT_DATOS_CONTACTO.set_downstream([AUT_DATOS_PAGADOR,AUT_DATOS_CONTRATANTE])
    AUT_DATOS_PAGADOR >> modelPubSubMessage('AUT_DATOS_PAGADOR')
    AUT_DATOS_CONTRATANTE >> modelPubSubMessage('AUT_DATOS_CONTRATANTE')
    AUT_DATOS_ASEGURADO.set_upstream([AUT_DATOS_CONTACTO,AUT_ELEMENTO_OBJETO_TP])
    AUT_DATOS_ASEGURADO >> modelPubSubMessage('AUT_DATOS_ASEGURADO')
    AUT_PORC_COMISION_REF_INTERMEDIARIO_MCT.set_downstream(AGENTES_INIT)
    AUT_FOLIO_AGENTE.set_downstream(AGENTES_INIT)
    AUT_REL_AGENTE_ENDOSO.set_downstream(AGENTE_NIVEL_POLIZA_VERSION)
    AUT_REL_AGENTE_ENDOSO >> modelPubSubMessage('AUT_RELACION_AGENTE_ENDOSO')
    AGENTE_NIVEL_POLIZA_VERSION.set_downstream([UDI_PRIMARIA,UDI_SECUNDARIA,AGENTE_SUPERVISOR,APP0,APP1])
    APP2.set_upstream([APP0, APP1])
    APP2.set_downstream(APP3)
    APP3.set_downstream(APP4)
    APP4.set_downstream(APP5)
    APP5.set_downstream(APP6)
    APP6.set_downstream(WAIT_TO_DELETE_NEC)
    UDI_PRIMARIA >> modelPubSubMessage('AUT_AGENTE_UDIPRIMARIA')
    UDI_SECUNDARIA >> modelPubSubMessage('AUT_AGENTE_UDISECUNDARIA')
    AGENTE_SUPERVISOR >> modelPubSubMessage('AUT_AGENTE_SUPERVISOR')
    APP6 >> modelPubSubMessage('AUT_AGENTE_PRODUCTOR')
    ##############FIN FLUJO AGENTES##########################

    ############## AUT_POLIZA ###############################
    CREATE_CLUSTER_POLIZA.set_downstream(AUT_POLIZA)
    AUT_POLIZA >> modelPubSubMessage('AUT_POLIZA')
    AUT_POLIZA.set_downstream(DELETE_CLUSTER_POLIZA)
    DELETE_CLUSTER_POLIZA.set_downstream(NEC1_END)
    ############## AUT_POLIZA ###############################
    WAIT_TO_DELETE_NEC.set_upstream([UDI_PRIMARIA,AGENTE_SUPERVISOR,UDI_SECUNDARIA,AUT_DATOS_CONTRATANTE, AUT_DATOS_PAGADOR,AUT_CONTRATO])
    WAIT_TO_DELETE_NEC.set_downstream(DELETE_CLUSTER_NEC1)
    DELETE_CLUSTER_NEC1.set_downstream(NEC1_END)
    NEC1_END.set_downstream(SEMANTICA_EMI_INIT)
    ############## FIN MODELADO NO ECONOMICO ###########################

    ############## INICIO MODELADO EMISION ECONOMICO ######################
    #TPIVOTE_ECO.set_downstream(DELAY_ECO)
    #DELAY_ECO.set_downstream(DELETE_CLUSTER_PIV_ECO)
    #DELETE_CLUSTER_PIV_ECO.set_downstream(CREATE_CLUSTER_ECO1)
    CREATE_CLUSTER_ECO1.set_downstream([AUT_COASEGURO, AUT_COMISION, AUT_COMISION_SERVICIOS_CONEXO, AUT_SERVICIO_CONEXO,\
        AUT_UDI, AUT_UDIS_CONEXOS, COASEGURO, AUT_PRIMAS, COMISION_CEDIDA])
    
    AUT_PRIMAS >> modelPubSubMessage('AUT_PRIMAS') >> sleepMinute('AUT_PRIMAS',9)  >> PASO_1_LIBII
    PASO_1_LIBII.set_downstream(PASO_2_LIBII)
    AUT_COASEGURO >> modelPubSubMessage('AUT_COASEGURO') >> sleepMinute('AUT_COASEGURO',9) >> PASO_2_LIBII    
    PASO_2_LIBII.set_upstream(PASO_1_LIBII)
    AUT_COMISION >> modelPubSubMessage('AUT_COMISION') >>  sleepMinute('AUT_COMISION',9) >> PASO_3_LIBII    
    AUT_SERVICIO_CONEXO >> modelPubSubMessage('AUT_SERVICIO_CONEXO') >> sleepMinute('AUT_SERVICIO_CONEXO',9) >> PASO_4_LIBII
    AUT_COMISION_SERVICIOS_CONEXO >> modelPubSubMessage('AUT_COMISION_SERVICIOS_CONEXOS') >> sleepMinute('AUT_COMISION_SERVICIOS_CONEXOS',9) >> PASO_5_LIBII
    AUT_UDI >> modelPubSubMessage('AUT_UDI') >> sleepMinute('AUT_UDI',9) >> PASO_6_LIBII
    AUT_UDIS_CONEXOS >> modelPubSubMessage('AUT_UDIS_CONEXOS') >> sleepMinute('AUT_UDIS_CONEXOS',9) >> PASO_7_LIBII    
    COMISION_CEDIDA >> modelPubSubMessage('AUT_COMISION_CEDIDA') >> sleepMinute('AUT_COMISION_CEDIDA',9) >> PASO_8_LIBII    
    COASEGURO.set_downstream(WAIT_TO_DELETE_ECO)
    COASEGURO >> modelPubSubMessage('AUT_COMPANIAS_COASEGURADORA')
    COASEGURO >> modelPubSubMessage('AUT_NOM_COMPANIAS_COASEGURADORA')
    ############## FIN MODELADO EMISION ECONOMICO ###########################
    

    ############## INICIO SEMANTICA EMISION NO ECONOMICO###############################
    CREATE_CLUSTER_NEC2.set_upstream(SEMANTICA_EMI_INIT)    
    CREATE_CLUSTER_NEC2.set_downstream([CIFRAS_CONTROL_MODELADO,AUT_COASEGURO_POLIZA,AUT_POLIZA_PAQUETE])
    AUT_SEMANTICA_NEC_P01.set_upstream([AUT_COASEGURO_POLIZA,AUT_POLIZA_PAQUETE,CIFRAS_CONTROL_MODELADO])
    AUT_SEMANTICA_NEC_P02 >> modelPubSubMessage('AUT_POLIZA_PAQUETE')
    AUT_SEMANTICA_NEC_P02.set_upstream(AUT_SEMANTICA_NEC_P01)
    AUT_SEMANTICA_NEC_P03.set_upstream(AUT_SEMANTICA_NEC_P02)
    AUT_SEMANTICA_NEC_P04.set_upstream(AUT_SEMANTICA_NEC_P03)
    AUT_SEMANTICA_NEC_P05.set_upstream(AUT_SEMANTICA_NEC_P04)
    AUT_SEMANTICA_NEC_P06.set_upstream(AUT_SEMANTICA_NEC_P05)
    AUT_SEMANTICA_NEC_P07.set_upstream(AUT_SEMANTICA_NEC_P06)
    AUT_SEMANTICA_NEC_P08.set_upstream(AUT_SEMANTICA_NEC_P07)
    AUT_SEMANTICA_NEC_P09.set_upstream(AUT_SEMANTICA_NEC_P08)
    AUT_SEMANTICA_NEC_P10.set_upstream(AUT_SEMANTICA_NEC_P09)
    AUT_SEMANTICA_NEC_P11.set_upstream(AUT_SEMANTICA_NEC_P10)
    AUT_SEMANTICA_NEC_P12.set_upstream(AUT_SEMANTICA_NEC_P11)
    AUT_SEMANTICA_NEC_P12.set_downstream(DELETE_CLUSTER_NEC2)
    DELETE_CLUSTER_NEC2.set_downstream(CREATE_CLUSTER_ECO2)
    ############## FIN SEMANTICA EMISION NO ECONOMICO##################################
   
    ############## INICIO SEMANTICA EMISION ECONOMICO##################################
    PASO_9_LIBII.set_upstream([PASO_2_LIBII, PASO_3_LIBII, PASO_4_LIBII, PASO_5_LIBII, PASO_6_LIBII, PASO_7_LIBII, PASO_8_LIBII])
    PASO_9_LIBII.set_downstream(PASO_10_LIBII)
    PASO_10_LIBII.set_downstream(PASO_11_LIBII)
    PASO_11_LIBII.set_downstream(PASO_12_LIBII)
    PASO_12_LIBII.set_downstream(PASO_13_LIBII)
    PASO_13_LIBII.set_downstream(PASO_14_LIBII)
    PASO_14_LIBII.set_downstream(PASO_15_LIBII)
    PASO_15_LIBII.set_downstream(PASO_16_LIBII)
    PASO_16_LIBII.set_downstream(WAIT_TO_DELETE_ECO)
    WAIT_TO_DELETE_ECO.set_downstream(DELETE_CLUSTER_ECO1)
    DELETE_CLUSTER_ECO1.set_downstream(SCALE_COB_DC)
    SCALE_COB_DC.set_downstream(ECO1_END)
    ECO1_END.set_downstream(CREATE_CLUSTER_ECO2)
    CREATE_CLUSTER_ECO2.set_downstream(PASO_17_LIBII)
    PASO_17_LIBII.set_downstream(PASO_18_LIBII)
    PASO_18_LIBII.set_downstream([PASO_19_LIBII, PASO_20_22_LIBII])
    PASO_20_22_LIBII.set_downstream(PASO_23_25_LIBII)    
    
    PASO_19_LIBII.set_downstream([REMOVE_PREV_EMI_COB_BK,CARGA_SEMANTICA_ECO_1])
    REMOVE_PREV_EMI_COB_BK.set_downstream(BACKUP_EMI_COB)
    BACKUP_EMI_COB.set_downstream(EMISION_BRECHAS_COBERTURA)
    EMISION_BRECHAS_COBERTURA.set_downstream(EMISION_BRECHAS_REMEDY_COB)
    EMISION_BRECHAS_REMEDY_COB.set_downstream(CIFRAS_PEOPLE_SOFT)

    PASO_23_25_LIBII.set_downstream([REMOVE_PREV_EMI_POL_BK,CARGA_SEMANTICA_ECO_2])
    REMOVE_PREV_EMI_POL_BK.set_downstream(BACKUP_EMI_POL)
    BACKUP_EMI_POL.set_downstream(EMISION_BRECHAS_POLIZA)
    EMISION_BRECHAS_POLIZA.set_downstream(EMISION_BRECHAS_REMEDY_POL)
    EMISION_BRECHAS_REMEDY_POL.set_downstream(CIFRAS_PEOPLE_SOFT)
    # Validacion cifras PEOPLE_SOFT
    CIFRAS_PEOPLE_SOFT.set_downstream(branch_cifras)
    branch_cifras.set_downstream(NOT_EXEC_CIFRAS)
    NOT_EXEC_CIFRAS.set_downstream(DELETE_CLUSTER_ECO2)
    branch_cifras.set_downstream(CC_EMISION_PS_1)
    CC_EMISION_PS_1.set_downstream(CC_EMISION_PS_2)
    CC_EMISION_PS_2.set_downstream(CC_EMISION_PS_3)
    CC_EMISION_PS_3.set_downstream(CC_EMISION_PS_4)
    CC_EMISION_PS_4.set_downstream(DELETE_CLUSTER_ECO2)
    
    DELETE_CLUSTER_ECO2.set_downstream(AUTOS_END)
    ############## FIN SEMANTICA EMISION ECONOMICO ####################################
    
    # SINIESTROS
    # SecciÃ³n malla Siniestros
    
    
    def modelPubSubMessageSiniestros(t):
        return PubSubPublishOperator(
            task_id='ModelMessage-{t}'.format(t=t),
            project=TOPIC_GCP_PROJECT_ID,
            topic=TOPIC,
            messages=[{'data': str(b64encode(json.dumps({'table': t, 'tipo_carga': TIPO_CARGA_SINIESTROS, 'date': TODAYF}).encode(
                'utf-8')), 'utf-8'), 'attributes': {'Content-Type': 'application/json'}}],
            create_topic=True,
            dag=dag,
        )
    
    CREATE_CLUSTER_PIV_SIN = CREATE_CLUSTER_TASK(
        'CLUSTER_PIV_SIN', CLUSTER_NAME_PIV_SIN, 20)
    CREATE_CLUSTER_SIN1 = CREATE_CLUSTER_TASK(
        'CREATE_CLUSTER_SIN1', CLUSTER_NAME_SIN1, 20)
    
    CREATE_CLUSTER_SIN2 = CREATE_CLUSTER_TASK('CREATE_CLUSTER_SIN2',CLUSTER_NAME_SIN1,50)#60
    
    TPIVOTE_SINIESTROS = DataProcSparkOperator(
        task_id='TPIVOTE_SINIESTROS',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-Tpivote_Siniestros.jar'],
        main_class='mx.com.gnp.datalake.pivote.siniestros.App',
        cluster_name=CLUSTER_NAME_PIV_SIN,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS, DIAS_DELTA]
    )

    DELAY_SINI = bash_operator.BashOperator(
        task_id='DELAY_SINI',
        bash_command='sleep 300s '
    )

    DELETE_CLUSTER_PIV_SIN = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_PIV_SIN',
        cluster_name=CLUSTER_NAME_PIV_SIN,
        region=REGION,
        project_id=''+PROJECT_ID+''
    )

    P01 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP01Paso1',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP01Paso1.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.AutSiniP01App',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    TESORERIA = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosTesoreria',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosTesoreria.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.app',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    P02 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP02aP03',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP02aP03.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.AutSiniP02',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    AUTSINIESTRO = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-Siniestros',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-Siniestros.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.app',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    RECUPERACION = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosRecuperacion',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRecuperacion.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.SINIESTROS',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    PERDIDATOTAL = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosPerdidaTotal',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosPerdidaTotal.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.app',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    AFECTADO = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosAfectado',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosAfectado.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.app',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    PAGOENTRECIAS = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosPagoEntreCias',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosPagoEntreCias.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.app',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )
    
    WAIT_TO_DELETE_SINI = bash_operator.BashOperator(
        task_id='WAIT_TO_DELETE_SINI',
        bash_command='sleep 300s '
    )
    
    DELETE_CLUSTER_SIN1 = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_SIN1',
        cluster_name=CLUSTER_NAME_SIN1,
        region=REGION,
        project_id=''+PROJECT_ID+''
    )
    
    P03_00 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP02aP03_P03_00',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP02aP03.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.AutSiniP030',
        cluster_name=CLUSTER_NAME_POLIZA,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    P03_02_04 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP02aP03_P03_02_04',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP02aP03.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.AutSiniP03P2toP4',
        cluster_name=CLUSTER_NAME_POLIZA,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )
    
    SINIESTROS = DummyOperator(
        task_id='SINIESTROS'
    )
    
    SINIESTROS_BRECHAS_1 = DataProcPySparkOperator(
        task_id='SINIESTROS_BRECHAS_1',
        region=REGION,
        pyfiles=['gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosBrechas/packages.zip',
                 'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosBrechas/spk_libreria_brechas.zip'],
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosBrechas/jobs/spk_aut_siniestros_brechas.py',
        cluster_name=CLUSTER_NAME_POLIZA,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosBrechas/configs/parameters_siniestro.json', '--env', 'gcp']
    )
    
    SALVAMENTO_ESTIMADO = DummyOperator(
        task_id='GNP-GDP-AUT-SiniestrosSalvamentoEstimado'
    )
    
    SALVAMENTO_ESTIMADO_PASO1 = DataProcPySparkOperator(
        task_id='salvamento_estimado_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosSalvamentoEstimado/jobs/salvamento_estimado.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosSalvamentoEstimado/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosSalvamentoEstimado/configs/paramSinSalva.json', '--env', 'gcp']
    )

    SALVAMENTO_ESTIMADO_PASO2 = DataProcPySparkOperator(
        task_id='salvamentos_v3_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosSalvamentoEstimado/jobs/Salvamentos_v3.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosSalvamentoEstimado/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosSalvamentoEstimado/configs/paramSinSalva.json', '--env', 'gcp']
    )
    
    SALVAMENTO_ESTIMADO_PASO3 = DataProcPySparkOperator(
        task_id='salvamentos_union_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosSalvamentoEstimado/jobs/salvamentos_union.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosSalvamentoEstimado/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosSalvamentoEstimado/configs/paramSinSalva_union.json', '--env', 'gcp']
    )
    
    DELAY_AUT_SINIESTRO_COB_AFECTA_INSUMO_COMP_PRE1 = bash_operator.BashOperator(
        task_id='DELAY_AUT_SINIESTRO_COB_AFECTA_INSUMO_COMP_PRE1',
        bash_command='sleep 600s '
    )
    
    MOV_MAN_TRAZA = DummyOperator(
        task_id='GNP-GDP-AUT-SiniestrosMovManTraza'
    )

    branch_reval = BranchPythonOperator(
        task_id='BRANCH_RVL_1_ETAPA',
        python_callable=get_day
    )

    MOV_MAN_TRAZA_PASO1 = DataProcPySparkOperator(
        task_id='mov_man_base_trz_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/jobs/mov_man_base_trz.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/configs/paramMovMan_par.json', '--env', 'gcp']
    )

    MOV_MAN_TRAZA_PASO2 = DataProcPySparkOperator(
        task_id='movMan_p2_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/jobs/movMan_p2.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/configs/paramMovMan_p2.json', '--env', 'gcp']
    )

    MOV_MAN_TRAZA_PASO3 = DataProcPySparkOperator(
        task_id='movMan_p3_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/jobs/movMan_p3.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/configs/paramMovManP3.json', '--env', 'gcp']
    )

    MOV_MAN_TRAZA_PASO4 = DataProcPySparkOperator(
        task_id='movMan_p4_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/jobs/movMan_p4.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/configs/paramMovMan_p4.json', '--env', 'gcp']
    )

    MOV_MAN_TRAZA_PASO5 = DataProcPySparkOperator(
        task_id='movMan_p5_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/jobs/movMan_p5.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMovmanTraza/configs/paramMovMan_p5.json', '--env', 'gcp']
    )
    
    NOT_EXEC_REVAL = DummyOperator(task_id='NOT_EXEC_REVAL')
    
    REVAL_SALV_PE_PASO1 = DataProcPySparkOperator(
        task_id='rvl_salv_pe_paso1',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/jobs/spk_rvlsv_paso_0.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/configs/paramRvl1.json', '--env', 'gcp']
    )

    REVAL_SALV_PE_PASO2 = DataProcPySparkOperator(
        task_id='rvl_salv_pe_paso2',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/jobs/spk_rvlsv_paso_1_union_all.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/configs/paramRvl2.json', '--env', 'gcp']
    )

    REVAL_SALV_PE_PASO3 = DataProcPySparkOperator(
        task_id='rvl_salv_pe_paso3',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/jobs/spk_rvlsv_paso_2_oper_comp.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/configs/paramRvl3.json', '--env', 'gcp']
    )

    REVAL_SALV_PE_PASO4 = DataProcPySparkOperator(
        task_id='rvl_salv_pe_paso4',
        region=REGION,
        main='gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/jobs/spk_rvlsv_paso_3_oper_no_mach.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/configs/paramRvl4.json', '--env', 'gcp']
    )

    REVAL_SALV_PE_PASO5 = DataProcPySparkOperator(
        task_id='rvl_salv_pe_paso5',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/jobs/spk_rvlsv_paso_4_union_all.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamentoPrimeraEtapa/configs/paramRvl5.json', '--env', 'gcp']
    )

    REVAL_SALVAMENTOS_PASO1 = DataProcPySparkOperator(
        task_id='spk_rvlsv_estimadosSiniestrosV7_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamento/jobs/spk_rvlsv_estimadosSiniestrosV7.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamento/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamento/configs/paramRevalEstimados.json', '--env', 'gcp'],
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    REVAL_SALVAMENTOS_PASO2 = DataProcPySparkOperator(
        task_id='spk_rvlsv_unionV2_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamento/jobs/spk_rvlsv_unionV2.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamento/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamento/configs/paramRevalEstimados.json', '--env', 'gcp']
    )
    REVAL_SALVAMENTOS_PASO3 = DataProcPySparkOperator(
        task_id='spk_rvlsv_ajuste_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamento/jobs/spk_rvlsv_ajuste.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamento/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosRvlSalvamento/configs/paramRevalEstimados.json', '--env', 'gcp']
    )

    MOV_MAN_HIS_2 = DataProcPySparkOperator(
        task_id='movMan_p2_his_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/jobs/movMan_p2.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/configs/paramMovMan_p2.json', '--env', 'gcp']
    )

    MOV_MAN_HIS_3 = DataProcPySparkOperator(
        task_id='movMan_p3_his_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/jobs/movMan_p3.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/configs/paramMovMan_p3.json', '--env', 'gcp']
    )

    MOV_MAN_HIS_4 = DataProcPySparkOperator(
        task_id='movMan_p4_his_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/jobs/movMan_p4.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/configs/paramMovMan_p4.json', '--env', 'gcp']
    )

    MOV_MAN_HIS_5 = DataProcPySparkOperator(
        task_id='movMan_p5_his_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/jobs/movMan_p5.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SinietrosMovManHistoricos/configs/paramMovMan_p5.json', '--env', 'gcp']
    )

    P04_01 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP04Paso1a2',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP04Paso1a2.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.AutSiniP04P2App',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )
    
    COMPONENTES_COMUN = DataProcPySparkOperator(
        task_id='union_all_movimientos_py',
        region=REGION,
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosComponentesComun/jobs/union_all_movimientos.py',
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosComponentesComun/packages.zip',
        cluster_name=CLUSTER_NAME_NEC1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosComponentesComun/configs/paramSiniestros.json', '--env', 'gcp', '--ejec', TIPO_CARGA_SINIESTROS],
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
    
    def get_tipoCargaMovMan(**kwargs):
        if TIPO_CARGA_SINIESTROS == "delta":
            return "NOT_EXEC_MovManHis"
        else:
            return "movMan_p2_his_py"
    
    branch_SiniestrosMovManHis = BranchPythonOperator(
        task_id='BRANCH_SiniestrosMovManHis',
        trigger_rule="all_done",
        python_callable=get_tipoCargaMovMan,
        provide_context=True
    )
    
    NOT_EXEC_MovManHis = DummyOperator(task_id='NOT_EXEC_MovManHis')
    
    P04_03 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP04Paso3a4',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP04Paso3a4.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.AutSiniP04P4App',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    P04_05 = DataProcPySparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP04Paso5a6',
        region='us-central1',
        main='gs://gnp-dlk-pro-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP04Paso5a6/jobs/App.py',
        pyfiles='gs://gnp-dlk-pro-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP04Paso5a6/packages.zip',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[
            "--config", "gs://gnp-dlk-pro-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP04Paso5a6/configs/app_config.json"]
    )

    P04_07 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP04Paso7a8',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP04Paso7a8.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.emision.siniestros.AutSiniestrosP04P8',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['full', 'delta']
    )

    P04_09 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP04Paso9a10',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP04Paso9a10.jar'],
        main_class='com.gnp.mx.insumos.autos.siniestros.App',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    P05_01 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP05Paso0a1',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP05Paso0a1.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.app',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS]
    )

    P05_02 = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-SiniestrosP05Paso2a3',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosP05Paso2a3.jar'],
        main_class='com.gnp.mx.insumos.autos.siniestros.App',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['full', 'delta']
    )
    
    REMOVE_PREV_SIN_COB_BK = bash_operator.BashOperator(
        task_id="REMOVE_PREV_SIN_COB_BK",
        bash_command="gsutil -m rm gs://gnp-dlk-pro-analitica/autos/siniestros/AUT_SINIESTRO_COB_AFECTA_V2/data_bk/* "
    )
    
    BACKUP_SIN_COB = bash_operator.BashOperator(
        task_id="BACKUP_SIN_COB",
        bash_command="gsutil -m cp gs://gnp-dlk-pro-analitica/autos/siniestros/AUT_SINIESTRO_COB_AFECTA_V2/data/* gs://gnp-dlk-pro-analitica/autos/siniestros/AUT_SINIESTRO_COB_AFECTA_V2/data_bk  "
    )
    
    REMOVE_PREV_SIN_POL_BK = bash_operator.BashOperator(
        task_id="REMOVE_PREV_SIN_POL_BK",
        bash_command="gsutil -m rm gs://gnp-dlk-pro-analitica/autos/siniestros/AUT_SINIESTRO_POLIZA/data_bk/* "
    )
    
    BACKUP_SIN_POL = bash_operator.BashOperator(
        task_id="BACKUP_SIN_POL",
        bash_command="gsutil -m cp gs://gnp-dlk-pro-analitica/autos/siniestros/AUT_SINIESTRO_POLIZA/data/* gs://gnp-dlk-pro-analitica/autos/siniestros/AUT_SINIESTRO_POLIZA/data_bk  "
    )
    
    SINIESTROS_BRECHAS_2 = DataProcPySparkOperator(
        task_id='SINIESTROS_BRECHAS_2',
        region=REGION,
        pyfiles='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosBrechas/packages.zip',
        main='gs://'+PROJECT_ID +
        '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosBrechas/jobs/spk_aut_siniestros_brechas_semantica.py',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_pyspark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=['--ruta_config_hdfs', 'gs://'+PROJECT_ID +
                   '-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosBrechas/configs/parameters_siniestro_semantica.json', '--env', 'gcp']
    )

    DELETE_CLUSTER_SIN2 = DataprocClusterDeleteOperator(
        task_id='DELETE_CLUSTER_SIN2',
        cluster_name=CLUSTER_NAME_SIN1,
        region = REGION,
        project_id=''+PROJECT_ID+''
    )
    
    BACKUP_AUTOS_SEMANTICAS = bash_operator.BashOperator(
        task_id="BACKUP_AUTOS_SEMANTICAS_"+TIPO_CARGA,
        bash_command="gsutil -m cp -r gs://gnp-dlk-pro-analitica/autos gs://gnp-dlk-pro-analitica/autos-respaldo-"+TIPO_CARGA+"-{{ds_nodash}} "
    )
    
    BACKUP_AUTOS = bash_operator.BashOperator(
        task_id='BACKUP_AUTOS_'+TIPO_CARGA,
        bash_command='gsutil -m cp -r gs://'+PROJECT_ID+'-modelado/autos gs://'+PROJECT_ID+'-modelado/autos-respaldo-'+TIPO_CARGA+'-{{ds_nodash}} '
    )
    
    MECRESERVA = DataProcSparkOperator(
        task_id='GNP-GDP-AUT-MecReserva',
        region=REGION,
        dataproc_spark_jars=[
            'gs://'+PROJECT_ID+'-workspaces/autos/siniestros/GNP-GDP-AUT-SiniestrosMecReserva.jar'],
        main_class='mx.com.gnp.datalake.gcp.autos.siniestros.AutMecReservaApp',
        cluster_name=CLUSTER_NAME_SIN1,
        dataproc_spark_properties={
            'spark.jars.packages': 'org.apache.spark:spark-avro_2.11:2.4.4'},
        arguments=[TIPO_CARGA_SINIESTROS])
    

    AUTOS_INIT.set_downstream(CREATE_CLUSTER_PIV_SIN)
    CREATE_CLUSTER_PIV_SIN.set_downstream(TPIVOTE_SINIESTROS)
    TPIVOTE_SINIESTROS.set_downstream(DELAY_SINI)
    DELAY_SINI.set_downstream(DELETE_CLUSTER_PIV_SIN)
    DELETE_CLUSTER_PIV_SIN.set_downstream(CREATE_CLUSTER_SIN1)
    CREATE_CLUSTER_SIN1.set_downstream(
        [P01, TESORERIA, AUTSINIESTRO, RECUPERACION, PERDIDATOTAL, AFECTADO, PAGOENTRECIAS, MECRESERVA])
    TESORERIA >> modelPubSubMessageSiniestros('AUT_SINIESTRO_MEC_TESORERIA')
    AUTSINIESTRO >> modelPubSubMessageSiniestros('AUT_SINIESTRO')
    RECUPERACION >> modelPubSubMessageSiniestros('AUT_SINIESTRO_RECUPERACION')
    PERDIDATOTAL >> modelPubSubMessageSiniestros('AUT_DATOS_PERDIDA_TOTAL')
    AFECTADO >> modelPubSubMessageSiniestros('AUT_SINIESTRO_AFECTADO')
    PAGOENTRECIAS >> modelPubSubMessageSiniestros('AUT_SINIESTROS_CON_PAGOS_ENTRE_CIAS')
    P02.set_upstream([P01, TESORERIA])
    WAIT_TO_DELETE_SINI.set_upstream([P02, AUTSINIESTRO, RECUPERACION, PERDIDATOTAL, AFECTADO, PAGOENTRECIAS, MECRESERVA])
    WAIT_TO_DELETE_SINI.set_downstream(DELETE_CLUSTER_SIN1)
    WAIT_TO_DELETE_SINI >> modelPubSubMessageSiniestros('DLK_MOVIMIENTO_SINIESTRO')
    WAIT_TO_DELETE_SINI >> modelPubSubMessageSiniestros('DLK_MOVIMIENTO_SINIESTRO_CONTABILIDAD')
    DELETE_CLUSTER_SIN1.set_downstream(CREATE_CLUSTER_POLIZA)
    CREATE_CLUSTER_POLIZA.set_downstream(P03_00)
    P03_00.set_downstream(P03_02_04)
    P03_00 >> modelPubSubMessageSiniestros('AUT_SINIESTRO_COB_AFECTA_INSUMO_PREV')
    AUT_SINIESTRO_COB_AFECTA_INSUMO_NVOS_ModelMessage = modelPubSubMessageSiniestros('AUT_SINIESTRO_COB_AFECTA_INSUMO_NVOS')
    P03_02_04.set_downstream(SINIESTROS_BRECHAS_1)
    SINIESTROS_BRECHAS_1.set_downstream(AUT_SINIESTRO_COB_AFECTA_INSUMO_NVOS_ModelMessage)
    AUT_SINIESTRO_COB_AFECTA_INSUMO_NVOS_ModelMessage.set_downstream(DELETE_CLUSTER_POLIZA)
    
    CREATE_CLUSTER_NEC2.set_downstream(SINIESTROS)
    SINIESTROS.set_downstream(SALVAMENTO_ESTIMADO)
    SALVAMENTO_ESTIMADO.set_downstream(SALVAMENTO_ESTIMADO_PASO1)
    SALVAMENTO_ESTIMADO_PASO1.set_downstream(SALVAMENTO_ESTIMADO_PASO2)
    SALVAMENTO_ESTIMADO_PASO2.set_downstream(SALVAMENTO_ESTIMADO_PASO3)
    
    AUT_SINIESTRO_COB_AFECTA_INSUMO_COMP_PRE1_ModelMessage = modelPubSubMessageSiniestros(
        'AUT_SINIESTRO_COB_AFECTA_INSUMO_COMP_PRE1')
    SALVAMENTO_ESTIMADO_PASO3.set_downstream(
        AUT_SINIESTRO_COB_AFECTA_INSUMO_COMP_PRE1_ModelMessage)
    AUT_SINIESTRO_COB_AFECTA_INSUMO_COMP_PRE1_ModelMessage.set_downstream(
        DELAY_AUT_SINIESTRO_COB_AFECTA_INSUMO_COMP_PRE1)
    DELAY_AUT_SINIESTRO_COB_AFECTA_INSUMO_COMP_PRE1.set_downstream(
        MOV_MAN_TRAZA)
    MOV_MAN_TRAZA.set_downstream(MOV_MAN_TRAZA_PASO1)
    MOV_MAN_TRAZA_PASO1.set_downstream(MOV_MAN_TRAZA_PASO2)
    MOV_MAN_TRAZA_PASO2.set_downstream(MOV_MAN_TRAZA_PASO3)
    MOV_MAN_TRAZA_PASO3.set_downstream(MOV_MAN_TRAZA_PASO4)
    MOV_MAN_TRAZA_PASO4.set_downstream(MOV_MAN_TRAZA_PASO5)
    MOV_MAN_TRAZA_PASO5.set_downstream(branch_reval)
    
    branch_reval.set_downstream(NOT_EXEC_REVAL)
    branch_reval.set_downstream(REVAL_SALV_PE_PASO1)
    REVAL_SALV_PE_PASO1.set_downstream(REVAL_SALV_PE_PASO2)
    REVAL_SALV_PE_PASO2.set_downstream(REVAL_SALV_PE_PASO3)
    REVAL_SALV_PE_PASO3.set_downstream(REVAL_SALV_PE_PASO4)
    REVAL_SALV_PE_PASO4.set_downstream(REVAL_SALV_PE_PASO5)

    NOT_EXEC_REVAL.set_downstream(REVAL_SALVAMENTOS_PASO1)
    REVAL_SALV_PE_PASO5.set_downstream(REVAL_SALVAMENTOS_PASO1)
    REVAL_SALVAMENTOS_PASO1.set_downstream(REVAL_SALVAMENTOS_PASO2)
    REVAL_SALVAMENTOS_PASO2.set_downstream(REVAL_SALVAMENTOS_PASO3)

    REVAL_SALVAMENTOS_PASO3.set_downstream(branch_SiniestrosMovManHis)
    branch_SiniestrosMovManHis.set_downstream(NOT_EXEC_MovManHis)
    branch_SiniestrosMovManHis.set_downstream(MOV_MAN_HIS_2)
    NOT_EXEC_MovManHis.set_downstream(COMPONENTES_COMUN)

    MOV_MAN_HIS_2.set_downstream(MOV_MAN_HIS_3)
    MOV_MAN_HIS_3.set_downstream(MOV_MAN_HIS_4)
    MOV_MAN_HIS_4.set_downstream(MOV_MAN_HIS_5)
    MOV_MAN_HIS_5.set_downstream(COMPONENTES_COMUN)
    COMPONENTES_COMUN.set_downstream(P04_01)
    P04_01.set_downstream(DELETE_CLUSTER_NEC2)
    
    CREATE_CLUSTER_SIN2.set_upstream([PASO_19_LIBII,PASO_23_25_LIBII])
    CREATE_CLUSTER_SIN2.set_downstream([P04_03,P04_05])
    P04_07.set_upstream([P04_03,P04_05])
    P04_07.set_downstream(P04_09)
    P04_09.set_downstream([REMOVE_PREV_SIN_COB_BK,CARGA_SEMANTICA_SIN_1])
    
    REMOVE_PREV_SIN_COB_BK.set_downstream(BACKUP_SIN_COB)
    BACKUP_SIN_COB.set_downstream(P05_01)
    
    P05_01.set_downstream(P05_02)
    P05_02.set_downstream([REMOVE_PREV_SIN_POL_BK,CARGA_SEMANTICA_SIN_2])
    REMOVE_PREV_SIN_POL_BK.set_downstream(BACKUP_SIN_POL)
    BACKUP_SIN_POL.set_downstream(SINIESTROS_BRECHAS_2)

    SINIESTROS_BRECHAS_2.set_downstream([DELETE_CLUSTER_SIN2,CARGA_SEMANTICA_SINIESTROS_POLIZA_BRECHAS,CARGA_SEMANTICA_SINIESTROS_COB_BRECHAS])
    DELETE_CLUSTER_SIN2.set_downstream([BACKUP_AUTOS,BACKUP_AUTOS_SEMANTICAS])
    BACKUP_AUTOS.set_upstream(AUTOS_END)
    BACKUP_AUTOS_SEMANTICAS.set_upstream(AUTOS_END)