# Ventas Retail — William Barboza

Proyecto final integrador: pipeline end-to-end Bronze → Silver → Gold con
Spark Declarative Pipelines (Lakeflow), orquestado con Databricks Jobs y
desplegado como Databricks Asset Bundle (DAB).

## Arquitectura

- **Bronze**: ingesta cruda incremental (STREAM) desde un Volume — Streaming Table
- **Silver**: limpieza, tipado y expectations — Streaming Table
- **Gold**: modelo dimensional en estrella (hechos + dimensiones) — Materialized View

## Entidades y diccionario de datos

### clientes (CSV)
| Campo | Tipo | Descripción |
|---|---|---|
| customer_id | Integer | Identificador único del cliente (PK) |
| nombre | String | Nombre del cliente |
| apellido | String | Apellido del cliente |
| email | String | Correo electrónico de contacto |
| ciudad | String | Ciudad de residencia |
| pais | String | País de residencia |
| fecha_registro | Date | Fecha de alta del cliente (yyyy-MM-dd) |
| segmento | String | Segmento comercial: Retail o Premium |

### productos (CSV)
| Campo | Tipo | Descripción |
|---|---|---|
| product_id | Integer | Identificador único del producto (PK) |
| nombre_producto | String | Nombre comercial del producto |
| categoria | String | Categoría del producto |
| subcategoria | String | Subcategoría del producto |
| precio_unitario | Decimal | Precio unitario de lista |
| proveedor | String | Proveedor del producto |
| stock_actual | Integer | Unidades disponibles en inventario |

### pedidos (JSON)
| Campo | Tipo | Descripción |
|---|---|---|
| order_id | Integer | Identificador único del pedido (PK) |
| customer_id | Integer | FK hacia clientes.customer_id |
| fecha_pedido | Date | Fecha en la que se realizó el pedido |
| canal_venta | String | Canal por el que se generó el pedido |
| estado_pedido | String | Estado: completado, en_proceso, cancelado |
| total_pedido | Decimal | Monto total del pedido |

### detalle_pedidos (JSON)
| Campo | Tipo | Descripción |
|---|---|---|
| order_item_id | Integer | Identificador único de la línea (PK) |
| order_id | Integer | FK hacia pedidos.order_id |
| product_id | Integer | FK hacia productos.product_id |
| cantidad | Integer | Unidades compradas de ese producto |
| precio_unitario | Decimal | Precio unitario aplicado en la venta |
| descuento | Decimal | Porcentaje de descuento aplicado (0 a 1) |

## Relación entre entidades

```
pedidos.customer_id        → clientes.customer_id
detalle_pedidos.order_id   → pedidos.order_id
detalle_pedidos.product_id → productos.product_id
```

## Catálogo, esquemas y tablas

| Capa | Catálogo | Esquema | Tablas |
|---|---|---|---|
| Landing (Volume) | proyecto_final | landing | raw_data (volumen) |
| Bronze | proyecto_final | bronze | clientes_raw, productos_raw, pedidos_raw, detalle_pedidos_raw |
| Silver | proyecto_final | silver | clientes, productos, pedidos, detalle_pedidos |
| Gold | proyecto_final | gold | dim_cliente, dim_producto, dim_fecha, fact_ventas |

## Modelo dimensional (Gold)

```
              dim_cliente
                   |
dim_producto — fact_ventas — dim_fecha
```

`fact_ventas` contiene las llaves foráneas `customer_key`, `product_key`,
`date_key` junto con las métricas: `cantidad`, `precio_unitario`,
`descuento` y `monto_total`. Grano: 1 fila por línea de detalle de pedido.

## Calidad de datos (Expectations)

Se aplican reglas de severidad `warn` (expect), `drop` (expect_or_drop) y
`fail` (expect_or_fail) en Silver y Gold:

- **Silver**: validez estructural y de formato por campo (PKs no nulas,
  formato de email, valores permitidos en enums, valores no negativos).
- **Gold**: integridad del modelo dimensional (FKs no nulas hacia las
  dimensiones, métricas de `fact_ventas` no negativas).

## Estructura del proyecto

```
ventas_retail_william_barboza/
├── databricks.yml
├── resources/
│   ├── ventas_retail.pipeline.yml   # Definición del Declarative Pipeline
│   └── ventas_retail.job.yml        # Job que orquesta el pipeline
├── src/
│   └── transformations/
│       ├── 01_bronze.py
│       ├── 02_silver.py
│       └── 03_gold.py
├── setup/
│   └── setup.ipynb                  # Crea catálogo, esquemas y volumen
└── dashboard/
    └── Executive Sales Dashboard.lvdash.json
```

## Setup previo (obligatorio antes de correr el pipeline)

Ejecutar el notebook en `setup/` para crear el catálogo, los esquemas
(`landing`, `bronze`, `silver`, `gold`) y el volumen de datos crudos:

```sql
CREATE CATALOG IF NOT EXISTS proyecto_final;
CREATE SCHEMA IF NOT EXISTS proyecto_final.landing;
CREATE SCHEMA IF NOT EXISTS proyecto_final.bronze;
CREATE SCHEMA IF NOT EXISTS proyecto_final.silver;
CREATE SCHEMA IF NOT EXISTS proyecto_final.gold;
CREATE VOLUME IF NOT EXISTS proyecto_final.landing.raw_data;
```

Los archivos crudos se depositan en:
```
/Volumes/proyecto_final/landing/raw_data/ventas_retail_william_barboza/{entidad}/
```

## Despliegue

```bash
databricks bundle validate
databricks bundle deploy -t dev
```

Para correr el job/pipeline, usar el panel de **Deployments** en Databricks
o:

```bash
databricks bundle run ventas_retail_job -t dev
```

## Dashboard

El dashboard `Executive Sales Dashboard.lvdash.json` consume directamente
las tablas Gold (`dim_cliente`, `dim_producto`, `dim_fecha`, `fact_ventas`)
con al menos 4 visualizaciones distintas.
