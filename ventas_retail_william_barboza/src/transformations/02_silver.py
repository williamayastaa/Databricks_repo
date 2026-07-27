from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, trim, lower

# -------------------------------------------------------------------------
# 1. SILVER: CLIENTES
# -------------------------------------------------------------------------
@dp.table(
    name="silver.clientes",
    comment="Tabla Silver de clientes limpia con expectations de calidad"
)
@dp.expect_or_fail("pk_customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect("valid_email_format", "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'")
@dp.expect_or_drop("valid_segmento", "segmento IN ('Retail', 'Premium')")
def clientes():
    return (
        spark.readStream.table("bronze.clientes_raw")
        .select(
            col("customer_id").cast("integer").alias("customer_id"),
            trim(col("nombre")).alias("nombre"),
            trim(col("apellido")).alias("apellido"),
            lower(trim(col("email"))).alias("email"),
            trim(col("ciudad")).alias("ciudad"),
            trim(col("pais")).alias("pais"),
            to_date(col("fecha_registro"), "yyyy-MM-dd").alias("fecha_registro"),
            trim(col("segmento")).alias("segmento")
        )
    )

# -------------------------------------------------------------------------
# 2. SILVER: PRODUCTOS
# -------------------------------------------------------------------------
@dp.table(
    name="silver.productos",
    comment="Tabla Silver de productos con precios y stock validados"
)
@dp.expect_or_fail("pk_product_id_not_null", "product_id IS NOT NULL")
@dp.expect_or_drop("precio_positive", "precio_unitario > 0")
@dp.expect("stock_non_negative", "stock_actual >= 0")
def productos():
    return (
        spark.readStream.table("bronze.productos_raw")
        .select(
            col("product_id").cast("integer").alias("product_id"),
            trim(col("nombre_producto")).alias("nombre_producto"),
            trim(col("categoria")).alias("categoria"),
            trim(col("subcategoria")).alias("subcategoria"),
            col("precio_unitario").cast("decimal(10,2)").alias("precio_unitario"),
            trim(col("proveedor")).alias("proveedor"),
            col("stock_actual").cast("integer").alias("stock_actual")
        )
    )

# -------------------------------------------------------------------------
# 3. SILVER: PEDIDOS
# -------------------------------------------------------------------------
@dp.table(
    name="silver.pedidos",
    comment="Tabla Silver de pedidos cabecera"
)
@dp.expect_or_fail("pk_order_id_not_null", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_estado", "estado_pedido IN ('completado', 'en_proceso', 'cancelado')")
@dp.expect("total_pedido_non_negative", "total_pedido >= 0")
def pedidos():
    return (
        spark.readStream.table("bronze.pedidos_raw")
        .select(
            col("order_id").cast("integer").alias("order_id"),
            col("customer_id").cast("integer").alias("customer_id"),
            to_date(col("fecha_pedido"), "yyyy-MM-dd").alias("fecha_pedido"),
            trim(col("canal_venta")).alias("canal_venta"),
            trim(col("estado_pedido")).alias("estado_pedido"),
            col("total_pedido").cast("decimal(10,2)").alias("total_pedido")
        )
    )

# -------------------------------------------------------------------------
# 4. SILVER: DETALLE_PEDIDOS
# -------------------------------------------------------------------------
@dp.table(
    name="silver.detalle_pedidos",
    comment="Tabla Silver de detalle de pedidos"
)
@dp.expect_or_fail("pk_order_item_id_not_null", "order_item_id IS NOT NULL")
@dp.expect_or_fail("fk_order_and_product_not_null", "order_id IS NOT NULL AND product_id IS NOT NULL")
@dp.expect_or_drop("cantidad_positive", "cantidad > 0")
def detalle_pedidos():
    return (
        spark.readStream.table("bronze.detalle_pedidos_raw")
        .select(
            col("order_item_id").cast("integer").alias("order_item_id"),
            col("order_id").cast("integer").alias("order_id"),
            col("product_id").cast("integer").alias("product_id"),
            col("cantidad").cast("integer").alias("cantidad"),
            col("precio_unitario").cast("decimal(10,2)").alias("precio_unitario"),
            col("descuento").cast("decimal(5,2)").alias("descuento")
        )
    )