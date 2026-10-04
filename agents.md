# Flujo de Trabajo Multi-Agente (Para GPT-6 Astra u otros orquestadores)

Este proyecto requiere perfiles especializados. Se propone la siguiente división de agentes:

## 1. 🐍 Agente de Ingestión y Scraping (Data Extractor)
- **Rol**: Desarrollador Python especializado en web scraping e integraciones API.
- **Responsabilidad**: 
  - Crear el script que extrae datos de la SBS (https://www.sbs.gob.pe/app/pp/sisti/paginas/diaria.aspx) y del BCR.
  - Integrar una API gratuita (ej. `yfinance`) para el tipo de cambio intradía.
  - Escribir el Kafka Producer para enviar los datos al tópico `fx-rates`.

## 2. ⚙️ Agente DevOps & Data Engineer Core
- **Rol**: Ingeniero de Infraestructura y Datos.
- **Responsabilidad**:
  - Escribir el `podman-compose.yml` que levante ZooKeeper, Kafka y Spark.
  - Configurar los volúmenes, redes de contenedores y variables de entorno.
  - Preparar los volúmenes persistentes de Delta Lake y los checkpoints.

## 3. ⚡ Agente Spark & Delta Lake (Streaming Processor)
- **Rol**: Especialista en Big Data.
- **Responsabilidad**:
  - Escribir el job de PySpark (`spark_streaming.py`).
  - Configurar `readStream` desde Kafka.
  - Implementar la escritura `writeStream` hacia Delta Lake (capas Bronze y Silver).
  - Mantener checkpoints separados y tablas Delta consultables desde Streamlit.

## 4. 📊 Agente Frontend & Analytics (Streamlit Dev)
- **Rol**: Desarrollador de Visualización de Datos.
- **Responsabilidad**:
  - Crear `app.py` con Streamlit.
  - Leer directamente la tabla Delta Gold usando la librería `deltalake` (delta-rs).
  - Construir gráficos de series de tiempo interactivos (Plotly/Altair) con auto-refresco (ej. `streamlit-autorefresh`).
