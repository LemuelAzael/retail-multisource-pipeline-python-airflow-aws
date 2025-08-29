from modules.read import Read
from modules.upload import Upload
from modules.enriched import Enriched
import pandas as pd


def run_transformation():
    """Lee datos de la capa landing, realiza enriquecimientos, KPIs y los carga a la capa processed."""
    
    extract = Read()
    load = Upload()
    transform = Enriched()

    # Lectura desde S3 (capa landing)
    print("=== INICIA LECTURA DE DATOS EN S3 (CAPA LANDING) ===")
    categories = extract.s3_read('retail-multisource-pipeline', 'landing/categories.csv')
    print('Leído: categories')

    customers = extract.s3_read('retail-multisource-pipeline', 'landing/customers.csv')
    print('Leído: customers')

    departments = extract.s3_read('retail-multisource-pipeline', 'landing/departments.csv')
    print('Leído: departments')

    order_items = extract.s3_read('retail-multisource-pipeline', 'landing/order_items.csv')
    print('Leído: order_items')

    orders = extract.s3_read('retail-multisource-pipeline', 'landing/orders.csv')
    print('Leído: orders')

    products = extract.s3_read('retail-multisource-pipeline', 'landing/products.csv')
    print('Leído: products')

    print("=== TERMINA LECTURA DE DATOS ===\n")

    # Enriquecimiento de datos
    print("=== INICIA ENRIQUECIMIENTO DE DATOS ===")
    enriched_categories = transform.enriched_categories(categories, departments)
    enriched_customers = transform.enriched_customers(customers)
    enriched_products = transform.enriched_products(products, enriched_categories)
    enriched_order_items = transform.enriched_order_items(order_items, enriched_products)
    enriched_orders = transform.enriched_orders(orders, enriched_order_items, enriched_customers)
    print("Enriquecimiento completado")

    # Carga de datos enriquecidos
    print("=== CARGA DE DATOS ENRIQUECIDOS A S3 (CAPA PROCESSED) ===")
    load.upload_amazon_s3_parquet(enriched_categories, 'retail-multisource-pipeline', 'processed/enriched/enriched_categories/enriched_categories.parquet')
    load.upload_amazon_s3_parquet(enriched_customers, 'retail-multisource-pipeline', 'processed/enriched/enriched_customers/enriched_customers.parquet')
    load.upload_amazon_s3_parquet(enriched_products, 'retail-multisource-pipeline', 'processed/enriched/enriched_products/enriched_products.parquet')
    load.upload_amazon_s3_parquet(enriched_order_items, 'retail-multisource-pipeline', 'processed/enriched/enriched_order_items/enriched_order_items.parquet')
    load.upload_amazon_s3_parquet(enriched_orders, 'retail-multisource-pipeline', 'processed/enriched/enriched_orders/enriched_orders.parquet')
    print("Carga de enriquecidos completada")

    # Cálculo de KPIs
    print("=== INICIA CÁLCULO DE KPIs ===")
    kpi_ventas_mes = transform.kpi_ventas_mes(enriched_orders)
    kpi_top_productos = transform.kpi_top_productos(enriched_order_items)
    ticket_promedio = transform.kpi_ticket_promedio(enriched_orders)
    clientes_recurrentes = transform.kpi_clientes_recurrentes(enriched_orders)
    ventas_estado = transform.kpi_ventas_estado(enriched_orders)
    print("KPIs calculados")

    # Carga de KPIs
    print("=== CARGA DE KPIs A S3 (CAPA PROCESSED) ===")
    load.upload_amazon_s3_parquet(kpi_ventas_mes, 'retail-multisource-pipeline', 'processed/kpis/kpi_ventas_mes/kpi_ventas_mes.parquet')
    load.upload_amazon_s3_parquet(kpi_top_productos, 'retail-multisource-pipeline', 'processed/kpis/kpi_top_productos/kpi_top_productos.parquet')
    load.upload_amazon_s3_parquet(ticket_promedio, 'retail-multisource-pipeline', 'processed/kpis/kpi_ticket_promedio/kpi_ticket_promedio.parquet')
    load.upload_amazon_s3_parquet(clientes_recurrentes, 'retail-multisource-pipeline', 'processed/kpis/kpi_clientes_recurrentes/kpi_clientes_recurrentes.parquet')
    load.upload_amazon_s3_parquet(ventas_estado, 'retail-multisource-pipeline', 'processed/kpis/kpi_ventas_estado/kpi_ventas_estado.parquet')
    print("Carga de KPIs completada")

    print("=== TRANSFORMACIÓN FINALIZADA ===")


if __name__ == "__main__":
    run_transformation()

