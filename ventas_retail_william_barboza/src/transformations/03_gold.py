from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, year, month, dayofmonth, dayofweek, quarter, 
    date_format, round as spark_round
)

# -------------------------------------------------------------------------
# 1. GOLD: DIMENSIÓN CLIENTE
# -------------------------------------------------------------------------
@dp.table(
    name="gold.dim_cliente",
    comment="Dimensión Cliente (Esquema Estrella) - 1 fila por cliente"
)
@dp.expect_or_fail("gold_pk_cliente_not_null", "customer_key IS NOT NULL")
def dim_cliente():
    return (
        spark.read.table("silver.clientes")
        .select(
            col("customer_id").alias("customer_key"),
            col("nombre"),
            col("apellido"),
            col("email"),
            col("ciudad"),
            col("pais"),
            col("fecha_registro"),
            col("segmento")
        )
    )

# -------------------------------------------------------------------------
# 2. GOLD: DIMENSIÓN PRODUCTO
# -------------------------------------------------------------------------
@dp.table(
    name="gold.dim_producto",
    comment="Dimensión Producto (Esquema Estrella) - 1 fila por producto"
)
@dp.expect_or_fail("gold_pk_producto_not_null", "product_key IS NOT NULL")
def dim_producto():
    return (
        spark.read.table("silver.productos")
        .select(
            col("product_id").alias("product_key"),
            col("nombre_producto"),
            col("categoria"),
            col("subcategoria"),
            col("precio_unitario"),
            col("proveedor"),
            col("stock_actual")
        )
    )

# -------------------------------------------------------------------------
# 3. GOLD: DIMENSIÓN FECHA
# -------------------------------------------------------------------------
@dp.table(
    name="gold.dim_fecha",
    comment="Dimensión Fecha derivada de los pedidos"
)
@dp.expect_or_fail("gold_pk_fecha_not_null", "date_key IS NOT NULL")
def dim_fecha():
    return (
        spark.read.table("silver.pedidos")
        .select("fecha_pedido")
        .distinct()
        .select(
            col("fecha_pedido").alias("date_key"),
            year(col("fecha_pedido")).alias("anio"),
            month(col("fecha_pedido")).alias("mes"),
            date_format(col("fecha_pedido"), "MMMM").alias("nombre_mes"),
            dayofmonth(col("fecha_pedido")).alias("dia"),
            dayofweek(col("fecha_pedido")).alias("dia_semana"),
            quarter(col("fecha_pedido")).alias("trimestre")
        )
    )

# -------------------------------------------------------------------------
# 4. GOLD: TABLA DE HECHOS - FACT_VENTAS
# -------------------------------------------------------------------------
@dp.table(
    name="gold.fact_ventas",
    comment="Tabla de Hechos de Ventas - Grano por línea de detalle de pedido"
)
@dp.expect_or_fail("gold_fk_cliente_not_null", "customer_key IS NOT NULL")
@dp.expect_or_fail("gold_fk_producto_not_null", "product_key IS NOT NULL")
@dp.expect_or_drop("gold_monto_total_non_negative", "monto_total >= 0")
def fact_ventas():
    pedidos_df = spark.read.table("silver.pedidos")
    detalle_df = spark.read.table("silver.detalle_pedidos")
    
    return (
        detalle_df.join(pedidos_df, on="order_id", how="inner")
        .select(
            col("order_item_id").alias("sales_item_key"),
            col("order_id"),
            col("customer_id").alias("customer_key"),
            col("product_id").alias("product_key"),
            col("fecha_pedido").alias("date_key"),
            col("canal_venta"),
            col("estado_pedido"),
            col("cantidad"),
            col("precio_unitario"),
            col("descuento"),
            spark_round(
                (col("cantidad") * col("precio_unitario")) * (1 - col("descuento")), 2
            ).alias("monto_total")
        )
    )