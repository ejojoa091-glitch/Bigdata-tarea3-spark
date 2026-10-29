# Tarea 3 - Procesamiento de Datos con Apache Spark
 *Curso:* Big Data (202016911)
 *Universidad:* UNAD - ECBTI
 *Estudiante:* Eduardo Esteban Jojoa
 *Grupo:* 94
 ---
 ## 📋 Descripción
 Este repositorio contiene el código desarrollado para la Tarea 3 del curso de Big Data. Se implementa el procesamiento de datos en lote y en tiempo real utilizando Apache Spark y Apache Kafka.
 El caso de estudio se centra en el conjunto de datos *"Accidentalidad Vial en Colombia"*, publicado por el Ministerio de Transporte en el portal de Datos Abiertos de Colombia.
 - *Fuente:* https://www.datos.gov.co/Transporte/Accidentalidad-Vial/
 - *Formato:* CSV
 - *Tamaño aproximado:* 245.680 registros
 ---
 ## 📁 Archivos incluidos
 | Archivo | Descripción |
 |---------|-------------|
 | procesamiento_batch.py | Análisis en lote: carga, limpieza y análisis exploratorio de datos |
 | productor_kafka.py | Generador de datos simulados que envía registros a Kafka cada 2 segundos |
 | consumidor_streaming.py | Consumidor Spark Streaming que procesa datos en tiempo real |
 ---
 ## 🚀 Requisitos de ejecución
 - Python 3.x
 - PySpark
 - Apache Kafka (Zookeeper + Broker)
 ### Crear el topic en Kafka:
 ```bash
 kafka-topics.sh --create --topic accidentes_tiempo_real --bootstrap-server localhost:9092
