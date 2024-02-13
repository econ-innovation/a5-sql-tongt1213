import mysql.connector
from mysql.connector import Error
import requests
import os
import time
import logging

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 从环境变量中获取敏感信息
db_user = os.getenv('DB_USER', 'root')
db_password = os.getenv('DB_PASSWORD', 'root123')
db_host = os.getenv('DB_HOST', 'localhost')
db_database = os.getenv('DB_DATABASE', 'company_management')
amap_key = os.getenv('AMAP_KEY', '5aac460935bced375af0a79957971ecd')

def get_addresses_from_db():
    """从数据库中获取地址信息"""
    try:
        conn = mysql.connector.connect(host=db_host, user=db_user, password=db_password, database=db_database)
        cursor = conn.cursor()
        cursor.execute("SELECT id, address FROM address_info")
        addresses = cursor.fetchall()
        return conn, cursor, addresses
    except Error as e:
        logging.error(f"Database error: {e}")
        return None, None, []

def update_address_info(conn, cursor, address_id, latitude, longitude):
    """更新数据库中的地址信息"""
    try:
        cursor.execute("UPDATE address_info SET latitude = %s, longitude = %s WHERE id = %s",
                       (latitude, longitude, address_id))
        conn.commit()
    except Error as e:
        logging.error(f"Failed to update database: {e}")

def fetch_geocode(address_text):
    """使用高德API获取地址的经纬度信息"""
    try:
        response = requests.get('https://restapi.amap.com/v3/geocode/geo',
                                params={'address': address_text, 'key': amap_key})
        data = response.json()
        if data['status'] == '1' and int(data['count']) > 0:
            location = data['geocodes'][0]['location']
            return location.split(',')
    except requests.RequestException as e:
        logging.error(f"Request error: {e}")
    return None, None

def main():
    conn, cursor, addresses = get_addresses_from_db()
    if conn is not None and cursor is not None:
        for address in addresses:
            address_id, address_text = address
            latitude, longitude = fetch_geocode(address_text)
            if latitude and longitude:
                update_address_info(conn, cursor, address_id, latitude, longitude)
                logging.info(f"Updated address ID {address_id} with latitude {latitude} and longitude {longitude}")
            else:
                logging.warning(f"Failed to get geocode for address ID {address_id}")

        cursor.close()
        conn.close()
    else:
        logging.error("Failed to connect to the database or fetch addresses.")

if __name__ == "__main__":
    main()