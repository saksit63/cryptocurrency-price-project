from kafka import KafkaConsumer
from json import loads
import time
import pandas as pd


def main():

    consumer = KafkaConsumer(
        'pirce_cryptocurrency',
        bootstrap_servers = "localhost:9093",
        auto_offset_reset='earliest',  # เริ่มอ่านจาก offset แรก
        enable_auto_commit=True,   # บันทึกว่า consumer ได้อ่านข้อมูลจนถึงไหนแล้ว
        value_deserializer = lambda x: loads(x.decode('utf-8'))
    )

    csv_file = "data/price_cryptocurrency.csv"

    df = pd.read_csv(csv_file)

    for message in consumer:
        data = message.value
        final_df = pd.DataFrame([data])
        final_df.to_csv(csv_file, mode = "a", header=False, index=False)


if __name__ == "__main__":
    main()
