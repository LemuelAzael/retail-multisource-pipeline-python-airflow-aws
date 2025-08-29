# Docker file para JupyterLab

# Imagen base
#FROM jupyter/base-notebook:latest

# Establece el directorio de trabajo dentro del contenedor
#WORKDIR /home/jovyan/project

# Copia los requirements desde la raíz del proyecto
#COPY requirements.txt .

# Instala las dependencias si el archivo existe
#RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto de Jupyter
#EXPOSE 8888


# Dockerfile para usar Airflow

# Imagen base de Airflow
FROM apache/airflow:2.9.1

# Copia y instala dependencias
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Establecer PYTHONPATH para que los scripts encuentren los módulos
ENV PYTHONPATH=/opt/airflow

# Carpeta de trabajo
WORKDIR /opt/airflow
