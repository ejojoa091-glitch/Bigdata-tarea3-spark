# ==========================================================
 # PRODUCTOR KAFKA — GENERADOR DE DATOS SIMULADOS
 # Estudiante: Eduardo Esteban Jojoa
 # Fecha: 28 de octubre de 2026
 # ==========================================================
 from kafka import KafkaProducer
 import json
 import time
 import random
 from datetime import datetime
 # Configuración del productor
 producer = KafkaProducer(
     bootstrap_servers=['localhost:9092'],
     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
     api_version=(0, 10, 1)
 )
 # Listas de valores posibles para la simulación
 departamentos = [
     "NARIÑO", "ANTIOQUIA", "CUNDINAMARCA", "VALLE DEL CAUCA",
     "SANTANDER", "ATLÁNTICO", "TOLIMA", "BOYACÁ"
 ]
 tipos_accidente = ["CHOQUE", "ATROPELLO", "VOLCADURA", "CAÍDA DE OCUPANTE", "INCENDIO"]
 niveles_gravedad = ["LEVE", "MODERADA", "GRAVE", "FATAL"]
 print("=" * 60)
 print("🚀 PRODUCTOR KAFKA — INICIADO")
 print("📡 Enviando datos al topic: accidentes_tiempo_real")
 print("⏹️  Presiona Ctrl+C para detener")
 print("=" * 60)
 contador = 0
 try:
     while True:
         # Generar datos aleatorios simulados
         dato = {
             "fecha_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             "departamento": random.choice(departamentos),
             "tipo_accidente": random.choice(tipos_accidente),
             "gravedad": random.choice(niveles_gravedad),
             "cantidad_heridos": random.randint(0, 5),
             "cantidad_vehiculos": random.randint(1, 4)
         }
         
         # Enviar el mensaje al topic de Kafka
         producer.send("accidentes_tiempo_real", value=dato)
         contador += 1
         print(f"📤 [{contador}] Enviado → {dato['fecha_hora']} | {dato['departamento']} | {dato['tipo_accidente']} | {dato['gravedad']}")
         
         # Esperar 2 segundos antes de enviar el siguiente dato
         time.sleep(2)
         
 except KeyboardInterrupt:
     print("\n" + "=" * 60)
     print("⏹️  Productor detenido por el usuario")
     print(f"✅ Total de mensajes enviados: {contador}")
     print("=" * 60)
 finally:
     producer.close()
     print("🔌 Conexión cerrada correctamente")