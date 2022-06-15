# DataEngineeringAssignment1

# Data Engineering
## Postgres to MongoDB (Airflow)

The main Idea of our exercise is to extract from Postgresql table and the produce json file is pushed to MongoDB database.

# Methodology

- Prepare the environment by using the docker-compose file ( attached in the repository) images (jupyter/minimal-notebook,airflow:2.0.1,postgres:13,redis,dpage/pgadmin4,mongo and mongo-express).
- Create CSV file by (jupyter).
- Upload the CSV file to postgress.
- extract the CSV file from postgress.
- read the extracted CSV file from postgress into panada.
- convert the panda to dict
- push dict to mongo BD
- Airflow DAG

## 1. Prepare the environment by using the docker-compose file

```sh
docker compose up
```
| App |user|password |Link |
| ------|----|------ | ------ |
| AirFlow-webServer|airflow|airflow |http://localhost:8887/|
| pgAdmin|psut@admin.com|psut2022 |http://localhost:8889/|
| JupyterLab|-|psut2022 |http://localhost:8886/|
| Mongo-express|psut|psut2022 |http://localhost:8888/|

## 2. Create CSV file by (jupyter):
using jupyter notebook to create CSV file by using faker package "generates fake data for you" check the below code:
![code1](https://user-images.githubusercontent.com/102326351/172030147-dc832fb3-a5e4-416f-aafd-1a88b6efbb21.PNG)
![code2](https://user-images.githubusercontent.com/102326351/172030160-71dce841-009d-4999-9ee9-fac044a6f9fb.PNG)

## 3. Upload the CSV file to postgress:
Connect to postgress :
```sh
from sqlalchemy import inspect,create_engine
import psycopg2

host="postgres_storage"
database="csv_db"
user="psut"
password="psut2022"
port='5432'
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
insp = inspect(engine)
print(insp.get_table_names())
```
upload csv file to postgress csv_db:

```sh
df=pd.read_csv('/home/sharedVol/data.csv')
df.to_sql('users2021', engine,if_exists='replace',index=False)
```
## 4. extract the CSV file from postgress & read the extracted CSV file from postgress into panada.:

```sh
dfp=pd.read_sql("SELECT * FROM users2021" , engine);
dfp.to_csv("/home/sharedVol/data2.csv")
```
## 5. convert the panda to dict & push dict to mongo BD:

```sh
#convert the pananda to Dict :
dfp.reset_index(inplace=True)
data_dict = dfp.to_dict("records")

#connect to Mongo db:
from pymongo import MongoClient
client = MongoClient('mongo:27017', username='psut',password='psut2022')
# create db and collection:
db = client['users2022']
collection = db['users']
#push to mongo collection:
collection.estimated_document_count()
```
## 6. Put the above steps as Airflow DAG:
```sh
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
from sqlalchemy import create_engine
from pymongo import MongoClient
import pandas as pd

host = "postgres_storage"
database = "csv_data"
user="psut"
password="psut2022"
port = '5432'
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

client = MongoClient('mongo:27017',
                     username='psut',
                     password='psut2022')
mongodb = client['users2022']
collection = mongodb['users']


def _read_table_as_DF():
    df = pd.read_sql("SELECT * FROM users2021;", engine)
    df.to_csv("/home/sharedVol/data2.csv")
    print(DF.head(5))


def _push_DF_to_Mongo():
    dfp = pd.read_csv("/home/sharedVol/data2.csv")
    dfp.reset_index(inplace=True)
    data_dict = DF2.to_dict("records")
    # Insert collection
    collection.insert_many(data_dict)


def _read_from_MongoDB():
    print('number of documents in mongoDB = ', collection.estimated_document_count());


def _install_tools():
    try:
        from faker import Faker
    except:
        subprocess.check_call(['pip', 'install', 'faker'])
        from faker import Faker

    try:
        import psycopg2
    except:
        subprocess.check_call(['pip', 'install', 'psycopg2-binary'])
        import psycopg2

    try:
        from sqlalchemy import create_engine
    except:
        subprocess.check_call(['pip', 'install', 'sqlalchemy'])
        from sqlalchemy import create_engine

    try:
        from pymongo import MongoClient
    except:
        subprocess.check_call(['pip', 'install', 'pymongo'])
        from pymongo import MongoClient

    try:
        import pandas as pd
    except:
        subprocess.check_call(['pip', 'install', 'pandas'])
        import pandas as pd


with DAG("etl_postgresql2mongo", start_date=datetime(2021, 1, 1),
         schedule_interval="*/10 * * * *", catchup=False) as dag:
    install_tools = PythonOperator(
        task_id="install_tools",
        python_callable=_install_tools
    )
    read_table_as_DF = PythonOperator(
        task_id="read_table_as_DF",
        python_callable=_read_table_as_DF
    )

    push_DF_to_Mongo = PythonOperataor(
        task_id="push_DF_to_Mongo",
        python_callable=_push_DF_to_Mongo
    )

    read_from_MongoDB = PythonOperator(
        task_id="read_from_MongoDB",
        python_callable=_read_from_MongoDB
    )

    install_tools >> read_table_as_DF >> push_DF_to_Mongo >> read_from_MongoDB
```
![Airflow](https://user-images.githubusercontent.com/102326351/172030975-8bec222f-32f6-4e83-89a1-709e41405df4.png)







