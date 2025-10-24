import time, json, pandas as pd
from kafka import KafkaProducer

data_path = "/home/vboxuser/bigdata_proyecto/datasets/infracciones_sincelejo.csv"
df = pd.read_csv(data_path)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Enviando datos al topic 'infracciones_data' (Ctrl+C para detener)")

while True:
    fila = df.sample(1).iloc[0]
    evento = {
        "codigo": str(fila[0]),
        "descripcion": str(fila[1]),
        "cantidad": int(fila[2])
    }
    producer.send("infracciones_data", value=evento)
    print("Enviado:", evento)
    time.sleep(1)
