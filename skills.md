# Skills & Requisitos Técnicos del Proyecto

Para ejecutar este proyecto con éxito, los agentes deben utilizar los siguientes "Skills" y patrones de diseño:

## 1. Podman & Contenedores
- **Uso estricto de Podman**: En lugar de `docker`, los comandos deben ser compatibles con `podman` y `podman-compose`.
- **Imágenes recomendadas**: 
  - Kafka: `confluentinc/cp-kafka` o `bitnami/kafka`
  - Spark: imagen oficial `apache/spark` (3.5.6, Scala 2.12, Java 17)

## 2. Kafka + Python
- **Librería**: `confluent-kafka` (Recomendada por rendimiento) o `kafka-python`.
- **Patrón**: Serialización a JSON (`json.dumps`). Manejo de callbacks para confirmar entrega (`on_delivery`).

## 3. PySpark Structured Streaming
- **Lectura de Kafka**: 
  ```python
  df = spark.readStream.format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:29092") \
      .option("subscribe", "fx-rates") \
      .load()
  ```
- **Parseo de JSON**: Uso de `from_json` y esquemas definidos (`StructType`).
- **Escritura a Delta**: Uso de `.format("delta")` y `.option("checkpointLocation", "...")`.

## 4. Delta Lake (OSS)
- **Optimización**: Manejo de compactación (`OPTIMIZE`) para evitar el problema de archivos pequeños generados por el streaming.
- **Lectura en Python**: Uso de la librería `deltalake` (Rust binding) que es súper ligera y permite leer Delta tables en Pandas/Streamlit sin necesidad de levantar un cluster Spark completo para la lectura.

## 5. Streamlit
- **Estado (Session State)**: Para manejar el historial de datos sin recargar todo.
- **Auto-refresco**: `st.empty()` con un loop `while True: time.sleep(1)` o librerías de terceros como `streamlit-autorefresh` para dar la sensación de tiempo real.

- **Despliegue del lector**: Streamlit se ejecutará en Podman con `fx-delta-data` montado en `/opt/spark/data:ro` para leer Gold; Windows no accede directamente al volumen Linux.
