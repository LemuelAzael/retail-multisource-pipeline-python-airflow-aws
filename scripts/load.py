import boto3
import time


def run_load():
    """Crea base de datos y crawler en AWS Glue, y ejecuta el crawler para disponibilizar datos en Athena."""
    
    glue = boto3.client('glue', region_name='us-east-1')  # Cliente de Glue
    
    # Parámetros
    s3_bucket = "retail-multisource-pipeline"
    processed_prefix = "processed/"
    database_name = "retail_processed"
    crawler_name = "crawler_retail_processed"
    iam_role = "arn:aws:iam::905936428907:role/service-role/AWSGlueServiceRole-retail-S3"

    # Crear base de datos
    try:
        glue.create_database(DatabaseInput={"Name": database_name})
        print(f"Base de datos '{database_name}' creada.")
    except glue.exceptions.AlreadyExistsException:
        print(f"Base de datos '{database_name}' ya existe.")

    # Crear o actualizar crawler
    try:
        glue.create_crawler(
            Name=crawler_name,
            Role=iam_role,
            DatabaseName=database_name,
            Targets={'S3Targets': [{'Path': f"s3://{s3_bucket}/{processed_prefix}"}]},
            TablePrefix="",
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        print(f"Crawler '{crawler_name}' creado.")
    except glue.exceptions.AlreadyExistsException:
        print(f"Crawler '{crawler_name}' ya existe. Actualizando configuración...")
        glue.update_crawler(
            Name=crawler_name,
            Role=iam_role,
            DatabaseName=database_name,
            Targets={'S3Targets': [{'Path': f"s3://{s3_bucket}/{processed_prefix}"}]},
            TablePrefix="",
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )

    # Ejecutar crawler
    print("Ejecutando crawler...")
    glue.start_crawler(Name=crawler_name)

    # Esperar finalización
    while True:
        status = glue.get_crawler(Name=crawler_name)['Crawler']['State']
        if status == 'READY':
            print("Crawler finalizado. Tablas listas en Glue/Athena.")
            break
        else:
            print("Crawler en ejecución, esperando 10s...")
            time.sleep(10)


if __name__ == "__main__":
    run_load()
