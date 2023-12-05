from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery import LoadJobConfig
from pandas_gbq import to_gbq
import pandas as pd

import re
import json
import os

from google_auth_oauthlib import flow


# ## Funciones:
# 
# - **credentials = service_account.Credentials.from_service_account_file(bq_credentials, scopes=scopes)**
# - **client = bigquery.Client(credentials=credentials, project=credentials.project_id)**   --> para establecer la conexion
# ---
# - **client.dataset(dataset_id)**  --> para referenciar el data set
# - **dataset = client.create_dataset('us_states_dataset')** --> sino existe el dataset lo podemos crear
# - **client.table(table_id)**   --> para referenciar la tabla
# - **table = client.create_table(table)** --> sino existe la tabla la podemos crear
# - o referenciarla de golpe:
# - **table = client.dataset(dataset_id).table(table_id)** --> para referenciar el dataset y la tabla en conjunto
# - en el caso de que si que exista:
# - **table = client.get_table(table)** --> obtenemos la tabla de BigQuery
# ---
# - **job_config = bigquery.job.QueryJobConfig(use_query_cache=False)**   --> para modificar configuraciones
# - **job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")**  --> para modificar configuraciones
# - **job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON** --> para seguir añadiendo modificaciones configuraciones
# ---
# - **client.query(query, job_config=job_config)** --> para ejecutar una query
# ---
# - **client.load_table_from_uri(gcs_uri, table, job_config=job_config)** --> para cargar una tabla a BQ desde un bucket de GCS
# ---
# - CREACION DE TABLA
# - - **table = client.dataset(dataset_id).table(table_id)** --> referenciamos la tabla que deberia existir en BigQuery
#   - 0º COMPROBAMOS  a ver si existe **client.get_table(table)** --> obtenemos la tabla de BigQuery. SINO EXISTE LA TENDRIAMOS QUE CREAR:
#   - 1º DEFINIMOS el schema --> **schema = [bigquery.SchemaField("cars", "STRING", mode="NULLABLE"), bigquery.SchemaField("mpg", "FLOAT", mode="REQUIRED"),  ...]**
#   - 2º DEFINIMOS la tabla --> **table = bigquery.Table(table, schema=schema)**
#   - 3º CREAMOS la tabla --> **table = client.create_table(table)**
#   - también se podría saltar el paso 1 y 2 y configurar el schema en el jobconfig
#   - - **job_config = job_config.schema = [bigquery.SchemaField('name', 'STRING'), bigquery.SchemaField('post_abbr','STRING')]**
#     - **table = client.create_table(table, job_config=job_config)**
# ---
# - INSERCION DE VALORES (debajo de los ya existentes) (partimos de que ya existe)
# - - 0º OBTENEMOS la tabla **table = client.get_table(table)**
#   - 1º CARGAMOS DATOS (desde un DF en este caso) **job = client.load_table_from_dataframe(DF, table)**
# --- 
# - SOBREESCRIBIR VALORES
# - - 0º OBTENEMOS la tabla **table = client.get_table(table)**
#   - 1º CAMBIAMOS la configuración **job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")**
#   - 2º   CARGAMOS DATOS (desde un DF en este caso) **job = client.load_table_from_dataframe(DF, table, job_config=job_config)**



## Configuración
# variables de configuración
config_env = open('../config/general_config_environments.json')
config_env = json.load(config_env)
env = config_env['general']['config_env']

config_file = open('../config/general_config_' + env + '.json')
config_file = json.load(config_file)
#BigQuery
scopes = config_file['bigquery']['scopes']
scopes = config_file['bigquery']['scopes']
bq_credentials = config_file['bigquery']['bq_credentials']
project_id = config_file['bigquery']['project_id']
dataset_id = config_file['bigquery']['dataset_id']
table_id = config_file['bigquery']['table_id']
csv_path = config_file['bigquery']['csv_path']
insert_method = config_file['bigquery']['insert_method']

#CONEXION
def bq_client(scopes, bq_credentials, project_id):
    try:
        credentials = service_account.Credentials.from_service_account_file(bq_credentials, scopes=scopes)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        return client
    except Exception as e:
        print("Big query conection wrong: " + str(e))

client = bq_client(scopes, bq_credentials, project_id)


#REFERENCIAR LA TABLA
def bq_table_ref(client,dataset_id,table_id):
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        return table_ref
    except Exception as e:
        print("Error in Bigquery table reference: " + str(e))

table = bq_table_ref(client,dataset_id,table_id)


#CSV a un DataFrame de Pandas
def bq_csv_to_pandas(csv_path):
    try:
        bigquery_csv = pd.read_csv(csv_path, sep=",", index_col=False)
        return bigquery_csv
    except Exception as e:
        print("Error importing the csv file" + str(e))

bq_csv_df = bq_csv_to_pandas(csv_path)


## CREAR Y SUBIR DATOS A BQ
#CREAR SQUEMA
def bq_schema_for_query(bq_csv_df):
    try:
        schema = eval(re.sub("\[|\]|dtype|\(|\)","",str(bq_csv_df.dtypes.replace("O","string").to_dict())))
        schema_bq = [bigquery.SchemaField(keyx, valuex, mode="NULLABLE") for keyx,valuex in schema.items()]
        return schema_bq
        
    except Exception as e:
        print(e)  

bq_schema_for_query = bq_schema_for_query(bq_csv_df)

#CREAR TABLA
def bq_python_create_table_dynamic(client,bq_csv_df,bq_schema_for_query, table, dataset_id, table_id):
    # Obtiene la tabla
    try:
        client.get_table(table)
        print(f"The table {table_id} already exists in the dataset {dataset_id}.")
    except Exception as e:
        if "Not found" in str(e): 
            # Crea la tabla si no existe
            table = bigquery.Table(table, schema=bq_schema_for_query)
    
            #para crear tabla particionada
            #table.time_partitioning = bigquery.TimePartitioning(
            #    type_=bigquery.TimePartitioningType.DAY,
            #    field="date",  # name of column to use for partitioning
            #    expiration_ms=1000 * 60 * 60 * 24 * 90,
            #)  # 90 days
            
            table = client.create_table(table)
            return("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

bq_python_create_table_dynamic(client,bq_csv_df, bq_schema_for_query, table, dataset_id, table_id)

#INSERTAR DATOS
def bq_python_insert_method(client, bq_csv_df, table, dataset_id, table_id, insert_method):
    if insert_method and insert_method == "replace":
        def bq_python_truncate_table(client, bq_csv_df, table, dataset_id, table_id):
            try:
                table = client.get_table(table)
                #print(f"The table {table_id} already exists in the dataset {dataset_id}.")
        
                #truncate
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
                #insertar datos
                job = client.load_table_from_dataframe(bq_csv_df, table, job_config=job_config)
                job.result()  # Espera a que se complete la carga
            
                print(f"CSV data replaced the existing table data: {table_id}")
            
            except:
                print(f"The table {table_id} does not exist in the dataset {dataset_id}, must be created")
                
        bq_python_truncate_table(client, bq_csv_df, table, dataset_id, table_id)

    elif insert_method and insert_method == "append":
        def bq_python_insert_values(client, bq_csv_df, table, dataset_id, table_id):
            try:
                table = client.get_table(table)
                #print(f"The table {table_id} already exists in the dataset {dataset_id}.")
            
                job = client.load_table_from_dataframe(bq_csv_df, table)
                job.result()  # Espera a que se complete la carga
            
                print(f"CSV data added to: {table_id}")
            except:
                print(f"The table {table_id} does not exist in the dataset {dataset_id}, must be created")
        
        bq_python_insert_values(client, bq_csv_df, table, dataset_id, table_id)

    else:
        raise ValueError("CSV data can not be uploaded, please review the config file and the insert_method variable or the ptyhon file") 

bq_python_insert_method(client, bq_csv_df, table, dataset_id, table_id, insert_method)


# ## OPCIÓN LIBRERÍA PANDAS to_gbp
# 
# def bq_python_to_gbp_create_and_replace_values(client, bq_csv_df, project_id, dataset_id, table_id, insert_method):     
#     try:
#         to_gbq(bq_csv_df, f'{project_id}.{dataset_id}.{table_id}', project_id=project_id, if_exists=insert_method)
#         return f"La tabla {table_id} ha sido creada y los datos del CSV han sido cargados en BigQuery."
#     except Exception as e:
#         print(f"Error: {e}")
# 
# bq_python_to_gbp_create_and_replace_values(client, bq_csv_df, project_id, dataset_id, table_id, insert_method)





