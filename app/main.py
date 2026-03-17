from fastapi import FastAPI
import mysql.connector
import os

app = FastAPI()

def get_db_connection():
    return mysql.connector.connect(
        host = os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT", "3311")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "root"),
        database=os.getenv("DB_NAME", "digital_hunter")
    )
