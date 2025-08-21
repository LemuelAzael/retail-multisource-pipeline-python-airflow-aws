from io import BytesIO
import pandas as pd

from utils import connections as c

class Upload:
    def __init__(self) -> None:
        self.process = 'Upload Process'
    
    # Para cargar a la capa Landing y capa Gold
    def upload_amazon_s3_csv(self, dataframe: pd.DataFrame, bucket_name: str, file_name: str) -> None:
        s3_client = c.get_amazon_s3_connection()  
        
        if not file_name.endswith(".csv"):
            file_name += ".csv"
        
        csv_buffer = BytesIO()
        dataframe.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        s3_client.upload_fileobj(csv_buffer, bucket_name, file_name)

    
    def upload_amazon_s3_parquet(self, dataframe: pd.DataFrame, bucket_name: str, file_name: str) -> None:
        s3_client = c.get_amazon_s3_connection()
        
        # Asegurar extensión .parquet
        if not file_name.endswith(".parquet"):
            file_name += ".parquet"
        
        # Guardar DataFrame en buffer en formato Parquet
        parquet_buffer = BytesIO()
        dataframe.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)
        
        # Subir a S3
        s3_client.upload_fileobj(parquet_buffer, bucket_name, file_name)