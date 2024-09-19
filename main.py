import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()


dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')

try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    print('Sucesso!')
except Exception as e:
    print(f"Erro de conexão")
