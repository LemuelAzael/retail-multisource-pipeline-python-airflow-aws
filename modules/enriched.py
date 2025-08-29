import pandas as pd

class Enriched:
    def __init__(self) -> None:
        self.process = 'Enriched Process'

    # ---------------------------
    # Transformaciones básicas y enriquecimiento
    # ---------------------------

    ## Categories + Departments
    def enriched_categories(self, categories, departments):
        if "category_name" in categories.columns:
            categories["category_name"] = categories["category_name"].str.strip().str.lower()
        
        return categories.merge(
            departments,
            left_on="category_department_id",
            right_on="department_id",
            how="left"
        )

    ## Customers
    def enriched_customers(self, customers):
        if {"customer_fname", "customer_lname"}.issubset(customers.columns):
            customers["customer_fullname"] = (
                customers["customer_fname"].str.strip().str.title() + " " +
                customers["customer_lname"].str.strip().str.title()
            )
        return customers

    ## Products + Categories
    def enriched_products(self, products, categories):
        products = products.merge(
            categories,
            left_on="product_category_id",
            right_on="category_id",
            how="left"
        )

        if "product_price" in products.columns:
            products["product_price"] = products["product_price"].astype(float)

        if "product_image" in products.columns:
            products["product_image_valid"] = products["product_image"].apply(
                lambda x: str(x).startswith("http")
            )

        return products

    ## Order Items + Products
    def enriched_order_items(self, order_items, products):
        if {"order_item_quantity", "order_item_product_price"}.issubset(order_items.columns):
            order_items["order_item_total"] = (
                order_items["order_item_quantity"] *
                order_items["order_item_product_price"]
            )

        return order_items.merge(
            products,
            left_on="order_item_product_id",
            right_on="product_id",
            how="left"
        )

    ## Orders + Customers + Order Items
    def enriched_orders(self, orders, order_items, customers):
        if "order_date" in orders.columns:
            orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
            orders["order_month"] = orders["order_date"].dt.month
            orders["order_year"] = orders["order_date"].dt.year

        if {"order_item_order_id", "order_item_total"}.issubset(order_items.columns):
            order_totals = (
                order_items.groupby("order_item_order_id")["order_item_total"]
                .sum()
                .reset_index()
                .rename(columns={
                    "order_item_order_id": "order_id",
                    "order_item_total": "order_total"
                })
            )
            orders = orders.merge(order_totals, on="order_id", how="left")

        return orders.merge(
            customers,
            left_on="order_customer_id",
            right_on="customer_id",
            how="left"
        )

    # ---------------------------
    # Agregaciones para KPIs
    # ---------------------------

    ## Ventas totales por mes
    def kpi_ventas_mes(self, orders):
        return (
            orders.groupby(["order_year", "order_month"])["order_total"]
            .sum()
            .reset_index()
        )

    ## Top 5 productos más vendidos
    def kpi_top_productos(self, order_items_df):
        return (
            order_items_df.groupby("product_name")["order_item_quantity"]
            .sum()
            .reset_index()
            .sort_values(by="order_item_quantity", ascending=False)
            .head(5)
        )

    ## Ticket promedio
    def kpi_ticket_promedio(self, orders):
        ticket_promedio = orders["order_total"].mean()
        return pd.DataFrame({"ticket_promedio": [ticket_promedio]})

    ## Clientes recurrentes
    def kpi_clientes_recurrentes(self, orders):
        columna_cliente = "customer_id" if "customer_id" in orders.columns else "order_customer_id"
        clientes_recurrentes = (
            orders.groupby(columna_cliente)["order_id"]
            .nunique()
            .reset_index()
        )
        return clientes_recurrentes[clientes_recurrentes["order_id"] > 1]

    ## Ventas por estado
    def kpi_ventas_estado(self, orders):
        if {"customer_state", "order_total"}.issubset(orders.columns):
            return orders.groupby("customer_state")["order_total"].sum().reset_index()
        return pd.DataFrame()  # Retorna vacío si no existen las columnas


