from modules.read import Read
from modules.upload import Upload
import pandas as pd


def run_ingestion():
    """Extrae datos de MySQL, MongoDB y ADLS, y los carga en S3 capa landing."""
    extract = Read()
    load = Upload()

    print("=== INICIA EXTRACCIÓN DE DATOS ===")

    # MySQL
    mysql_categories_df = extract.mysql_read('categories')
    mysql_customers_df = extract.mysql_read('customers')
    print("Datos extraídos desde MySQL")

    # MongoDB
    mongodb_departments_df = extract.mongodb_read('retail_db', 'departments')
    mongodb_order_items_df = extract.mongodb_read('retail_db', 'order_items')
    print("Datos extraídos desde MongoDB")

    # ADLS
    adls_orders_df = extract.adls_read('retail-source', 'orders.csv')
    adls_products_df = extract.adls_read('retail-source', 'products.csv')
    print("Datos extraídos desde ADLS")

    print("=== TERMINA EXTRACCIÓN DE DATOS ===\n")

    print("=== INICIA CARGA DE DATOS A S3 (CAPA LANDING) ===")

    load.upload_amazon_s3_csv(mysql_categories_df, 'retail-multisource-pipeline', 'landing/categories.csv')
    print("Cargado: categories.csv")

    load.upload_amazon_s3_csv(mysql_customers_df, 'retail-multisource-pipeline', 'landing/customers.csv')
    print("Cargado: customers.csv")

    load.upload_amazon_s3_csv(mongodb_departments_df, 'retail-multisource-pipeline', 'landing/departments.csv')
    print("Cargado: departments.csv")

    load.upload_amazon_s3_csv(mongodb_order_items_df, 'retail-multisource-pipeline', 'landing/order_items.csv')
    print("Cargado: order_items.csv")

    load.upload_amazon_s3_csv(adls_orders_df, 'retail-multisource-pipeline', 'landing/orders.csv')
    print("Cargado: orders.csv")

    load.upload_amazon_s3_csv(adls_products_df, 'retail-multisource-pipeline', 'landing/products.csv')
    print("Cargado: products.csv")

    print("=== TERMINA CARGA DE DATOS A S3 (CAPA LANDING) ===")


if __name__ == "__main__":
    run_ingestion()
