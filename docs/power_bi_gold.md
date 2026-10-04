# Gold para Finanzas, Ventas y Operaciones

Silver conserva una fila limpia y tipada por línea de orden, aplica contratos y reglas de negocio, y sirve como base reutilizable. Gold transforma ese detalle en productos analíticos con un grano explícito, dimensiones compartidas y métricas acordadas. La capa Gold del proyecto genera tablas Delta y una publicación CSV compacta para Power BI Desktop; el dashboard y el archivo PBIX se construyen en Power BI.

## Qué puede responder el negocio

| Área | Preguntas y métricas | Alcance |
|---|---|---|
| Finanzas | Importe antes de impuestos, descuentos, impuestos e importe con impuestos por mes | Importes de órdenes; no cobros ni rentabilidad |
| Ventas | Evolución mensual, segmentos, clientes e importe por orden | Fecha de la orden, no fecha de despacho |
| Operaciones | Cantidad de líneas, unidades y estados de órdenes | Estado observado; no tiempo de entrega ni inventario |

El `net_revenue` de Silver incluye impuestos. En reportes debe presentarse como **importe con impuestos**, nunca como utilidad o efectivo cobrado. El precio base ya es el importe extendido de la línea TPC-H: no se multiplica nuevamente por `quantity`. Gold calcula el importe antes de impuestos, descuento e impuesto con aritmética decimal y redondeo monetario. La politica monetaria redondea `revenue_before_tax = base_price * (1 - discount_rate)`
a la escala del `money_type` configurado en YAML; `discount_amount` es la diferencia
entre base e importe antes de impuestos. `tax_amount` es la diferencia entre el
`net_revenue` canonico de Silver y el importe antes de impuestos. Asi los centavos
reconcilian exactamente, incluso en casos de medio centavo; el descuento publicado
puede diferir en un centavo de redondear `base_price * discount_rate` por separado.
Los totales se suman a partir de estos importes por linea, sin recalcular impuestos
sobre el total agregado. TPC-H no especifica una moneda en el contrato actual:
no asignar PEN o USD sin acordarlo con el negocio.

## Dos modelos de consumo

La exportación CSV predeterminada contiene `dim_date`, `dim_customer`, `dim_order_status`, `dim_market_segment`, `agg_sales_monthly` y `agg_customer_monthly`. Permite trabajar con el volumen agregado sin importar millones de líneas. Las agregaciones representan el mes mediante su primer día. En este modelo usa filtros por mes y año completos; una selección de días arbitrarios no representa ventas diarias. Para análisis diario importa fact_sales.

`agg_sales_monthly` agrupa por mes, segmento y estado; `agg_customer_monthly` por mes y cliente. Usa cada tabla para sus visuales correspondientes. **No sumes los importes de ambas:** son dos representaciones de las mismas ventas. Tampoco relaciones directamente las dos agregaciones.

Para análisis por producto, orden o línea, agrega las tablas de detalle al listado de exportación en el YAML y vuelve a ejecutar Gold: `fact_sales`, `fact_orders`, `dim_product`, y las dimensiones compartidas necesarias. `fact_sales` tiene una fila por `(order_id, line_number)`; `fact_orders`, una por `order_id`. El importe total de la orden pertenece a `fact_orders` y no debe sumarse desde el detalle de líneas. `dim_product` contiene solamente identificadores observados: aún no existe una fuente `part` con nombre, marca o categoría.

La dimensión de clientes refleja los atributos presentes en el snapshot; no implementa un histórico SCD2. Las métricas incluyen solo órdenes representadas en el Silver actual. El estado describe el snapshot observado y no ofrece historia de cambios.

## Cargar en Power BI Desktop

1. Ejecuta el notebook Gold después de finalizar Silver. Verifica la publicación `exports/gold/current.json` en el host; las rutas `/opt/spark/...` pertenecen al contenedor.
2. En **Transformar datos → Administrar parámetros**, crea `pGoldFolder`, de tipo Texto, con la ruta Windows absoluta a `exports\gold` del repositorio.
3. Crea una **Consulta en blanco**, abre **Editor avanzado**, pega el contenido de [power_bi_gold.pq](power_bi_gold.pq) y llama a la consulta `fxGoldTable`. Deshabilita su carga.
4. Crea una consulta en blanco por tabla. Por ejemplo: `= fxGoldTable("dim_date")`. Usa exactamente los nombres del manifiesto como nombres de consulta.
5. Aplica cambios, configura relaciones y crea las medidas. Desactiva las relaciones automáticas que conecten hechos entre sí.

La función lee exclusivamente la versión señalada por el manifiesto, combina archivos `part-*.csv`, interpreta UTF-8 y aplica tipos explícitos del esquema publicado. No combines recursivamente todas las versiones del directorio `releases`: duplicarías datos. El [conector de carpetas de Microsoft](https://learn.microsoft.com/es-es/power-query/connectors/folder) permite combinar múltiples archivos con el mismo esquema.

Los decimales se importan como **Número decimal fijo** (`Currency.Type`). Microsoft especifica cuatro posiciones decimales y un rango limitado para este tipo; Gold verifica el límite configurado antes de publicar. Una medida que sume muchas filas también puede superar ese rango, aunque cada fila individual sea válida. Las tasas porcentuales y los ratios de medidas deben formatearse como porcentajes, y los importes con dos decimales. [Tipos de datos de Power BI](https://learn.microsoft.com/en-us/power-bi/connect-data/desktop-data-types).

## Relaciones y calendario

Configura relaciones **uno a muchos** con dirección de filtro única desde cada dimensión hacia los hechos. Es el patrón recomendado por Microsoft para [modelos estrella](https://learn.microsoft.com/en-us/power-bi/guidance/star-schema).

En el modelo compacto, usa estas relaciones:

| Dimensión (lado 1) | Hecho (lado muchos) | Clave en ambos |
|---|---|---|
| `dim_date` | Ambas agregaciones | `date_key` |
| `dim_market_segment` | `agg_sales_monthly` | `market_segment` |
| `dim_order_status` | `agg_sales_monthly` | `order_status` |
| `dim_customer` | `agg_customer_monthly` | `customer_id` |

Un filtro de estado no aplica a la agregación de clientes, porque su grano no conserva estado; para ese cruce se necesita detalle o una nueva agregación. Para visualizar clientes por segmento, usa `dim_customer[market_segment]`.

En el modelo de detalle, fecha, cliente y estado filtran `fact_sales` y `fact_orders`; producto filtra solo `fact_sales`. Nunca conectes `fact_sales` con `fact_orders`. Un filtro de producto no filtra el total completo de órdenes: usa medidas derivadas de `fact_sales` para ese análisis.

Marca `dim_date` como tabla de fechas usando `dim_date[date]`. El calendario es diario y continuo, incluso cuando el hecho agregado solo contiene el primer día del mes. Ordena `month_name` por `month`; `year_month` usa el formato `yyyy-MM`, cuyo orden alfabético coincide con el temporal. Deshabilita fecha/hora automática para este modelo.

## Reglas para medidas

Los importes, descuentos, impuestos, unidades y líneas son aditivos dentro de cada hecho. Los clientes distintos **no** son aditivos entre meses o estados. Nunca uses `SUM(customer_count)` como clientes únicos del periodo. Para clientes activos en el modelo compacto, cuenta los `customer_id` distintos de `agg_customer_monthly` bajo su contexto de fechas; para clientes únicos por estado utiliza `fact_sales`.

Los conteos de órdenes de `agg_sales_monthly` son aditivos bajo el contrato actual: una orden tiene una fecha, un cliente, un segmento y un estado constantes. No generalices esa propiedad a agregaciones futuras por producto: una orden puede tener varios productos. En detalle utiliza `DISTINCTCOUNT(fact_sales[order_id])`, y evita `COUNTROWS(fact_sales)` para contar órdenes.

El ticket promedio divide el importe antes de impuestos entre el número de órdenes, usando numerador y denominador del mismo hecho y contexto. El descuento ponderado divide el importe de descuento entre el importe base; promediar `discount_rate` daría el mismo peso a líneas de importes diferentes. El impuesto efectivo divide impuestos entre el importe antes de impuestos.

Ejemplos listos para el modelo compacto; crea cada medida por separado:

```dax
Importe antes de impuestos = SUM(agg_sales_monthly[revenue_before_tax])
Importe con impuestos = SUM(agg_sales_monthly[net_revenue])
Descuentos = SUM(agg_sales_monthly[discount_amount])
Impuestos = SUM(agg_sales_monthly[tax_amount])
Órdenes = SUM(agg_sales_monthly[order_count])
Unidades = SUM(agg_sales_monthly[quantity])
Ticket promedio = DIVIDE([Importe antes de impuestos], [Órdenes])
Descuento ponderado = DIVIDE([Descuentos], SUM(agg_sales_monthly[base_price]))
Importe por cliente = SUM(agg_customer_monthly[revenue_before_tax])
Clientes activos = DISTINCTCOUNT(agg_customer_monthly[customer_id])
Importe año anterior =
    CALCULATE([Importe antes de impuestos], DATEADD(dim_date[date], -1, YEAR))
Variación interanual % =
    DIVIDE([Importe antes de impuestos] - [Importe año anterior], [Importe año anterior])
```

`Clientes activos` responde a fecha y cliente/segmento de `dim_customer`; no a estado ni a `dim_market_segment`. Si no hay año anterior, la comparación queda en blanco por `DIVIDE`. La sintaxis basada en columna de `DATEADD` exige fechas contiguas en el contexto. [Referencia oficial de DATEADD](https://learn.microsoft.com/es-es/dax/dateadd-function-dax).

Para el modelo de detalle, cambia los importes a `SUM(fact_sales[...])` y crea `Órdenes detalle = DISTINCTCOUNT(fact_sales[order_id])` y `Clientes detalle = DISTINCTCOUNT(fact_sales[customer_id])`. El total ERP independiente puede exponerse como `Total orden ERP = SUM(fact_orders[order_total])`; su conciliación con importes de líneas es un indicador separado, no una garantía del modelo.

## Publicación y actualización

Cada ejecución escribe una publicación nueva y publica el manifiesto después de completar las tablas y sus controles. Si falla, el manifiesto anterior permanece disponible. Conserva las versiones mientras existan consumidores leyéndolas. No ejecutes otra publicación Gold durante la actualización de Power BI: distintas consultas pueden evaluar `current.json` en momentos diferentes. Para operar concurrentemente, fija un manifiesto de versión para toda la actualización del modelo.

La conexión CSV es **Import**, con actualización completa, y no representa DirectQuery ni actualización incremental de Power BI. Para servir grandes hechos con plegado de consultas, el siguiente paso es un endpoint SQL compatible sobre Delta en la plataforma elegida, con sus credenciales y conectores correspondientes.

En Power BI Service, una ruta local Windows requiere un gateway con acceso a esos archivos y permisos de lectura. Programa Gold antes de la actualización del modelo; subir el PBIX por sí solo no programa la ingesta. Una carpeta sincronizada por OneDrive en el disco sigue siendo una fuente local para esta consulta. Consulta la [documentación del gateway](https://learn.microsoft.com/en-gb/power-bi/connect-data/service-gateway-onprem) y la [actualización de datos](https://learn.microsoft.com/en-us/power-bi/connect-data/refresh-data).
