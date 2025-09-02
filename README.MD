# 🛒 Retail Data Pipeline using Airflow and AWS

Este proyecto implementa un pipeline de ingeniería de datos para centralizar, procesar y disponibilizar información proveniente de múltiples fuentes heterogéneas (MySQL, Azure Data Lake Storage (ADLS) y MongoDB) sobre un data lake en AWS, demostrando habilidades en orquestación, transformación de datos y análisis eficiente en la nube.

## 🧠 Descripción General

El pipeline permite la ingesta de datos crudos desde distintas fuentes hacia un bucket de Amazon S3 (Landing), su posterior transformación y enriquecimiento mediante Python y Airflow en Docker, y la disponibilidad de los datos procesados para consultas SQL con Amazon Athena.

Se generan datasets enriquecidos y KPI calculados, almacenados en la capa Processed en formato Parquet, optimizando el rendimiento y costos de análisis. Los datos procesados son catalogados con AWS Glue, permitiendo exploración y análisis de manera eficiente.

## 🏗️ Arquitectura del Proyecto

El flujo de trabajo combina servicios en la nube y procesamiento local:

1.  **Extracción e Ingesta** 
    -   Scripts en Python leen datos de MySQL, ADLS y MongoDB.       
    -   Se escriben datos crudos en **S3 Landing**.
        
2.  **Transformación y Enriquecimiento**
    -   Scripts de transformación y enriquecimiento en Python.     
    -   Airflow orquesta los DAGs asegurando ejecución secuencial y automática.
        
3.  **Capa Procesada**
    -   Datos transformados se escriben en **S3 Processed** en **Parquet**.
    -   Se generan subcarpetas de `enriched` y `kpis`.
        
4.  **Catalogación y Consulta**
    -   AWS Glue registra las tablas de datos procesados.
    -   Amazon Athena permite consultas SQL y análisis exploratorio.
  
## 🧰 Tecnologías y Herramientas

-   Lenguaje: Python 3.x
-   Orquestador: Apache Airflow
-   Contenedores: Docker
-   Almacenamiento y análisis: Amazon S3, AWS Glue, Amazon Athena
-   Fuentes de datos: MySQL, ADLS, MongoDB
-   Formato optimizado: Parquet

## 📁 Estructura del Repositorio

```text
retail-multisource-pipeline-python-airflow-aws/
├── dags/
│   └── etl_pipeline_dag.py
├── diagrams/
│   └── arquitectura_retail_multisource_pipeline.png
├── modules/
│   ├── __init__.py
│   ├── enriched.py
│   ├── read.py
│   └── upload.py
├── notebooks/
│   ├── data_enriched_kpis.ipynb
│   ├── data_ingestion.ipynb
│   ├── data_load.ipynb
│   ├── data_read.ipynb
│   ├── data_seeding.ipynb
│   └── data_transform.ipynb
├── scripts/
│   ├── ingest.py
│   ├── load.py
│   └── transform.py
├── utils/
│   └── connections.py
├── .dockerignore
├── .env-ejemplo
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## ☁️ Estructura del Data Lake en S3

```text
s3://retail-multisource-pipeline/
├── landing/                  # Datos crudos sin transformar
├── processed/                # Datos transformados
│   ├── enriched/
│   │   ├── enriched_categories/
│   │   │   └── enriched_categories.parquet
│   │   ├── enriched_customers/
│   │   │   └── enriched_customers.parquet
│   │   ├── enriched_products/
│   │   │   └── enriched_products.parquet
│   │   ├── enriched_order_items/
│   │   │   └── enriched_order_items.parquet
│   │   └── enriched_orders/
│   │       └── enriched_orders.parquet
│   │    
│   └── kpis/
│       ├── kpi_ventas_mes/
│       │   └── kpi_ventas_mes.parquet
│       ├── kpi_top_productos/
│       │   └── kpi_top_productos.parquet
│       ├── kpi_ticket_promedio/
│       │   └── kpi_ticket_promedio.parquet
│       ├── kpi_clientes_recurrentes/
│       │   └── kpi_clientes_recurrentes.parquet
│       └── kpi_ventas_estado/
│           └── kpi_ventas_estado.parquet
└── athena-query-results/     # Resultados temporales de queries
```

## ⚙️ Requisitos
-   Docker >= 20.x
-   Docker Compose >= 1.29.x  
-   Python >= 3.9 (solo si deseas ejecutar scripts localmente)    
-   Acceso a AWS con permisos para S3, Glue y Athena

## ⚙️ Instrucciones de Uso

1.  Configura las credenciales de AWS con permisos para S3, Glue y Athena.

3.  Crea el bucket principal `retail-multisource-pipeline` en S3 y asegúrate de que tenga las carpetas `landing` y `processed`.

4. Haz el poblado de datos del archivo `retail_db` en cada una de las fuentes externas (**MySQL, ADLS, MongoDB**).

5.  Clona el repositorio y navega al directorio del proyecto:
   `git clone https://github.com/LemuelAzael/retail-multisource-pipeline-python-airflow-aws.git cd retail-multisource-pipeline-python-airflow-aws` 

6.  Construye los contenedores Docker:
`docker-compose build` 

7.  Levanta los servicios:
    `docker-compose up -d` 

8.  Accede a **Airflow UI** en tu navegador:
    `http://localhost:8080` 

9.  Ejecuta los DAGs para que se realice la ingesta, transformación y carga de los datos.
    
10.  Verifica que los datasets procesados se encuentren en **Processed**, organizados en las carpetas `enriched` y `kpis`.
    
11.  Consulta los datos mediante **Amazon Athena**, usando las tablas registradas en **Glue Data Catalog**.


## 🔄 Pipeline de Datos

| Etapa               | Servicio / Script              | Descripción                                                  |
| ------------------- | ------------------------------ | ------------------------------------------------------------ |
| Fuentes de datos    | MySQL, ADLS, MongoDB           | Contienen datos crudos de retail.                            |
| Ingesta             | scripts/ingest.py + Landing S3 | Copia los datos originales al bucket de landing.             |
| Transformación      | scripts/transform.py           | Limpieza, transformación y enriquecimiento de datos.         |
| Procesamiento       | Airflow DAGs                   | Coordina ejecución secuencial de scripts.                    |
| Almacenamiento      | S3 Processed                   | Guarda datos transformados en Parquet (`enriched` y `kpis`). |
| Catalogación        | AWS Glue Data Catalog          | Registra los datos procesados como tablas para consultas.    |
| Consulta y análisis | Amazon Athena                  | Permite consultas SQL y análisis exploratorio.               |


## 🎯 Objetivos del Proyecto
-   Integrar y centralizar datos de múltiples fuentes heterogéneas.    
-   Diseñar pipelines automatizados, reproducibles y escalables.    
-   Transformar, limpiar y enriquecer datos de forma eficiente.    
-   Catalogar y disponibilizar datos para análisis SQL en la nube.    
-   Aplicar buenas prácticas de ingeniería de datos en un entorno profesional.

## 👤 Autor
Lemuel Azael Carrillo Barrera  
Proyecto para portafolio de ingeniería de datos.

🔗 [Sobre mí](https://www.notion.so/Perfil-profesional-Lemuel-Azael-Carrillo-Barrera-2619ec6ab8528025b300d6099cd92add?source=copy_link)

## 📚 Desarrollo completo del proyecto

Revisa todo el proceso paso a paso: desde la configuración inicial hasta la ejecución automatizada en AWS.

🔎 [Ver desarrollo completo en Notion](https://www.notion.so/Data-Pipeline-for-Retail-using-Airflow-and-AWS-2619ec6ab8528051a6d2c7d1dbcf24ed?source=copy_link)

## 🌐 Portafolio completo

Explora más proyectos y recursos en mis portafolios personales:

🔗 [GitHub – Portafolio](https://github.com/LemuelAzael/LemuelAzael)
🔗 [Notion – Portafolio](https://www.notion.so/Portafolio-Lemuel-Carrillo-2179ec6ab8528029ba54f3bf3363f993?source=copy_link)
