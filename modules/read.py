import os
from io import StringIO
import pandas as pd
from sqlalchemy import text

from utils import connections as c

class Read:
    def __init__(self) -> None:
        self.process = 'Read Process'
    
    # MySQL
    def mysql_read(self, table_name: str) -> pd.DataFrame:
        if not table_name.isidentifier():
            raise ValueError(f"Nombre de tabla no válido: {table_name}")
        
        engine = c.get_mysql_connection()
        with engine.connect() as conn:
            query = text(f"SELECT * FROM {table_name}")
            dataframe = pd.read_sql_query(query, conn)
        
        return dataframe
    
    # MongoDB
    def mongodb_read(self, database_name: str, collection_name: str) -> pd.DataFrame:
        dbname = c.get_mongo_connection(database_name)
        collection = dbname[collection_name]
        documents = list(collection.find({}))
        return pd.DataFrame(documents)
    
    # ADLS
    def adls_read(self, container_name: str, file_name: str) -> pd.DataFrame:
        container_client = c.get_adls_connection(container_name)
        blob_client = container_client.get_blob_client(file_name)
        csv_content = blob_client.download_blob().content_as_text(encoding="utf-8")
        return pd.read_csv(StringIO(csv_content))
    
    # Amazon S3 
    def s3_read(self, bucket_name: str, file_key: str) -> pd.DataFrame:
        s3_client = c.get_amazon_s3_connection()  
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        body = csv_obj['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(body))
		   