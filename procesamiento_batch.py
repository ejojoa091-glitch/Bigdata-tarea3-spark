1 # ==========================================================
2 # PROCESAMIENTO EN BATCH CON APACHE SPARK
3 # Conjunto de Datos: Accidentalidad Vial en Colombia
4 # Estudiante: Eduardo Esteban Jojoa
5 # Fecha: 28 de octubre de 2026
6 # ==========================================================
7 from pyspark.sql import SparkSession
8 from pyspark.sql.functions import col, count, desc, to_date, month, year
9 import matplotlib.pyplot as plt
10 import pandas as pd
11 # --------------------------
12 # PASO 1: Iniciar Sesión de Spark
13 # --------------------------
14 spark = SparkSession.builder \
15     .appName("Analisis_Accidentalidad_Colombia") \
16     .master("local[*]") \
17     .getOrCreate()
18 print("=" * 60)
19 print("SESIÓN DE SPARK INICIADA CORRECTAMENTE")
20 print("=" * 60)
21 # --------------------------
22 # PASO 2: Cargar el conjunto de datos
23 # --------------------------
24 ruta_archivo = "accidentes_viales_colombia.csv"
25 df = spark.read.csv(
26     ruta_archivo,
27     header=True,
28     inferSchema=True,
29     sep=";
30 )
31 print("\n📋 ESQUEMA DE LOS DATOS CARGADOS:")
32 df.printSchema()
33 print("\n📊 PRIMEROS 5 REGISTROS:")
34 df.show(5, truncate=False)
35 total_registros = df.count()
36 print(f"\n📌 TOTAL DE REGISTROS CARGADOS: {total_registros:,}")
 # --------------------------
 # PASO 3: Limpieza de datos
 # --------------------------
 print("\n" + "=" * 60)
 print("🧹 INICIANDO PROCESO DE LIMPIEZA DE DATOS")
 print("=" * 60)
 df_limpio = df.na.drop(subset=["FECHA", "DEPARTAMENTO", "TIPO_ACCIDENTE", "GRAVEDAD"])
 df_limpio = df_limpio.withColumn("FECHA", to_date(col("FECHA"), "dd/MM/yyyy"))
 df_limpio = df_limpio.filter(col("FECHA").isNotNull())
 registros_finales = df_limpio.count()
 registros_eliminados = total_registros - registros_finales
 print(f"✅ Registros iniciales: {total_registros:,}")
 print(f"✅ Registros eliminados por limpieza: {registros_eliminados:,}")
 print(f"✅ Registros finales disponibles: {registros_finales:,}")
 # --------------------------
 # PASO 4: Análisis Exploratorio de Datos (EDA)
 # --------------------------
 print("\n" + "=" * 60)
 print("📈 ANÁLISIS EXPLORATORIO DE DATOS — RESULTADOS")
 print("=" * 60)
 # Análisis 1: Accidentes por Departamento
 print("\n🔹 ANÁLISIS 1 — Top 10 Departamentos con más accidentes:")
 analisis_departamento = df_limpio.groupBy("DEPARTAMENTO") \
     .agg(count("*").alias("CANTIDAD_ACCIDENTES")) \
     .orderBy(desc("CANTIDAD_ACCIDENTES"))
 analisis_departamento.show(10, truncate=False)
 # Análisis 2: Distribución por Tipo de Accidente
 print("\n🔹 ANÁLISIS 2 — Distribución por Tipo de Accidente:")
 analisis_tipo = df_limpio.groupBy("TIPO_ACCIDENTE") \
     .agg(count("*").alias("CANTIDAD")) \
     .orderBy(desc("CANTIDAD"))
 analisis_tipo.show(truncate=False)
 # Análisis 3: Distribución por Nivel de Gravedad
 print("\n🔹 ANÁLISIS 3 — Distribución por Gravedad:")
 analisis_gravedad = df_limpio.groupBy("GRAVEDAD") \
     .agg(count("*").alias("CANTIDAD")) \
     .orderBy(desc("CANTIDAD"))
 analisis_gravedad.show(truncate=False)
 # Análisis 4: Comportamiento por Año
 df_tiempo = df_limpio.withColumn("ANIO", year("FECHA"))
 print("\n🔹 ANÁLISIS 4 — Accidentes por Año:")
 analisis_anio = df_tiempo.groupBy("ANIO") \
     .agg(count("*").alias("CANTIDAD_ACCIDENTES")) \
     .orderBy("ANIO")
 analisis_anio.show()
 # --------------------------
 # PASO 5: Visualización de Resultados
 # --------------------------
 print("\n🖼️ GENERANDO VISUALIZACIONES...")
 pd_departamento = analisis_departamento.limit(8).toPandas()
 pd_tipo = analisis_tipo.toPandas()
 pd_gravedad = analisis_gravedad.toPandas()
 pd_anio = analisis_anio.toPandas()
 # Gráfico 1: Top Departamentos
 plt.figure(figsize=(10, 6))
 plt.barh(pd_departamento["DEPARTAMENTO"], pd_departamento["CANTIDAD_ACCIDENTES"], color="#1f77b4")
 plt.xlabel("Cantidad de Accidentes")
 plt.ylabel("Departamento")
 plt.title("Top 8 Departamentos con Mayor Número de Accidentes")
 plt.gca().invert_yaxis()
 plt.tight_layout()
 plt.savefig("grafico_departamentos.png", dpi=300)
 print("✅ Gráfico 1 guardado: grafico_departamentos.png")
 # Gráfico 2: Tipo de Accidente
 plt.figure(figsize=(8, 6))
 colores = ["#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564f"]
 plt.pie(pd_tipo["CANTIDAD"], labels=pd_tipo["TIPO_ACCIDENTE"], autopct="%1.1f%%", colors=colores)
 plt.title("Distribución por Tipo de Accidente")
 plt.tight_layout()
 plt.savefig("grafico_tipo_accidente.png", dpi=300)
 print("✅ Gráfico 2 guardado: grafico_tipo_accidente.png")
 # Gráfico 3: Gravedad
 plt.figure(figsize=(8, 6))
 plt.bar(pd_gravedad["GRAVEDAD"], pd_gravedad["CANTIDAD"], color="#d62728")
 plt.xlabel("Nivel de Gravedad")
 plt.ylabel("Cantidad de Accidentes")
 plt.title("Accidentes según Nivel de