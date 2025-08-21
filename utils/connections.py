import os
from dotenv import load_dotenv

from sqlalchemy import create_engine
from pymongo import MongoClient
from azure.storage.blob import ContainerClient
import boto3

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# M y S Q L 
def get_mysql_connection():
    connection_string = os.getenv("MYSQL_CONNECTION_STRING")
    engine = create_engine(connection_string)
    return engine

# M O N G O D B 
def get_mongo_connection(database_name):
    connection_string = os.getenv("MONGO_URI")
    client = MongoClient(connection_string)
    dbname = client[database_name]
    return dbname

# A D L S
def get_adls_connection(container_name):
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_client = ContainerClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name
    )
    return container_client

# S 3
def get_amazon_s3_connection():
    s3_client = boto3.client('s3')
    return s3_client
