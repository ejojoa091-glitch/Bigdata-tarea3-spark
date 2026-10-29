# ==========================================================
 # CONSUMIDOR SPARK STREAMING — PROCESAMIENTO EN TIEMPO REAL
 # Estudiante: Eduardo Esteban Jojoa
 # Fecha: 28 de octubre de 2026
 # ==========================================================
 from pyspark.sql import SparkSession
 from pyspark.sql.functions import col, from_json, sum, count
 from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 # Crear sesión de Spark
 spark = SparkSession.builder \
     .appName("Streaming_Accidentes_Tiempo_Real") \
     .getOrCreate()
 # Reducir mensajes de registro en consola
 spark.sparkContext.setLogLevel("WARN")
 print("=" * 60)
 print("✅ SESIÓN DE SPARK STREAMING INICIADA")
 print("📡 Conectando al topic: accidentes_tiempo_real")
 print("⏹️  Presiona Ctrl+C para detener")
 print("=" * 60)
 # Definir el esquema de los datos que recibiremos
 esquema_datos = StructType([
     StructField("fecha_hora", StringType(), True),
     StructField("departamento", StringType(), True),
     StructField("tipo_accidente", StringType(), True),
     StructField("gravedad", StringType(), True),
     StructField("cantidad_heridos", IntegerType(), True),
     StructField("cantidad_vehiculos", IntegerType(), True)
 ])
 # Leer el flujo de datos desde Kafka
 flujo_datos = spark.readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", "localhost:9092") \
     .option("subscribe", "accidentes_tiempo_real") \
     .option("startingOffsets", "latest") \
     .load()
 # Convertir los datos binarios a formato JSON y aplicar el esquema
 datos_parseados = flujo_datos.selectExpr("CAST(value AS STRING)") \
     .select(from_json(col("value"), esquema_datos).alias("accidente")) \
     .select("accidente.*")
 # --------------------------
 # ANÁLISIS EN TIEMPO REAL
 # --------------------------
 # Análisis 1: Conteo de reportes por Departamento
 conteo_por_departamento = datos_parseados.groupBy("departamento") \
     .agg(count("*").alias("total_reportes")) \
     .orderBy(col("total_reportes").desc())
 # Análisis 2: Total de heridos por Tipo de Accidente
 heridos_por_tipo = datos_parseados.groupBy("tipo_accidente") \
     .agg(sum("cantidad_heridos").alias("total_heridos"))
 # Análisis 3: Conteo de reportes por Nivel de Gravedad
 conteo_por_gravedad = datos_parseados.groupBy("gravedad") \
     .agg(count("*").alias("cantidad_eventos"))
 # --------------------------
 # SALIDA DE RESULTADOS A CONSOLA
 # --------------------------
 # Salida 1: Reportes por Departamento
 consola_dep = conteo_por_departamento.writeStream \
     .outputMode("complete") \
     .format("console") \
     .option("truncate", "false") \
     .option("numRows", 8) \
     .queryName("📊 Reportes por Departamento") \
     .start()
 # Salida 2: Heridos por Tipo de Accidente
 consola_tipo = heridos_por_tipo.writeStream \
     .outputMode("complete") \
     .format("console") \
     .option("truncate", "false") \
     .queryName("📊 Heridos por Tipo de Accidente") \
     .start()
 # Salida 3: Distribución por Gravedad
 consola_gravedad = conteo_por_gravedad.writeStream \
     .outputMode("complete") \
     .format("console") \
     .option("truncate", "false") \
     .queryName("📊 Eventos por Gravedad") \
     .start()
 # Mantener la sesión activa hasta que el usuario la detenga
 spark.streams.awaitAnyTermination()