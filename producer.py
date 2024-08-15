from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager #จัดการเวอร์ชันของ WebDriver โดยอัตโนมัติ
from selenium.webdriver.common.by import By
from datetime import datetime
import time
from kafka import KafkaProducer
from json import dumps


def main():

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install())) #เปิด Chrome Driver ด้วยคำสั่ง webdriver.Chrome()

    driver.get("https://coinmarketcap.com/") #เปิดหน้าเวปไซต์

    def cryptocurrency_func():
        bitcoin = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[1]/div[2]/div/div[1]/div[4]/table/tbody/tr[1]/td[4]/div/span").text #ดึงข้อมูลตาม xpath ในรูปแบบ text
        ethereum = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[1]/div[2]/div/div[1]/div[4]/table/tbody/tr[2]/td[4]/div/span").text
        dogcoin = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[1]/div[2]/div/div[1]/div[4]/table/tbody/tr[9]/td[4]/div/span").text
        bnb = driver.find_element(By.XPATH, "/html/body/div[1]/div[2]/div[1]/div[2]/div/div[1]/div[4]/table/tbody/tr[4]/td[4]/div/span").text
        data = {
            "datetime": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "bitcoin": float(bitcoin.strip('$').replace(',', '')),
            "ethereum": float(ethereum.strip('$').replace(',', '')),
            "dogcoin": float(dogcoin.strip('$').replace(',', '')),
            "bnb": float(bnb.strip('$').replace(',', '')),
        }
        return data
    
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9093"],
        value_serializer=lambda x: dumps(x).encode('utf-8')  # ฟังก์ชันที่ใช้ในการแปลงข้อมูลเป็น JSON และเข้ารหัสเป็น UTF-8
    )

    while True:
        producer.send(topic='pirce_cryptocurrency', value=cryptocurrency_func()) # ส่งข้อความ
        time.sleep(10) #หน่วงเวลาในการส่ง


if __name__ == "__main__":
    main()
