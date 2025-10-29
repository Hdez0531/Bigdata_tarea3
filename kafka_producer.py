from kafka import KafkaProducer
import json
import random
import time

# Crear el productor y conectar con Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Enviar datos simulados de temperatura y humedad
while True:
    data = {
        "temperatura": round(random.uniform(20, 30), 2),
        "humedad": round(random.uniform(40, 60), 2),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send('sensor_data', value=data)
    print(f"Enviado: {data}")
    time.sleep(2)
