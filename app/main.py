from fastapi import FastAPI, HTTPException
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

@app.get("/movement_alert")
def movement_alert():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = '''
            SELECT entity_id, target_name, priority_level, movement_distance_km
            FROM targets
            WHERE priority_level IN (1,2) AND movement_distance_km > 5
            '''
        cursor.execute(query)
        results = cursor.fetchall
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail= str(e))
    finally:
        cursor.close
        conn.close
        
@app.get("/signal_type_sorted")
def signal_type_sorted():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = '''
            SELECT signal_type, COUNT(*) AS count
            FROM intel_signals
            GROUP BY signal_type
            ORDER BY count DESC
            '''
        cursor.execute(query)
        results = cursor.fetchall
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail= str(e))
    finally:
        cursor.close
        conn.close
        
@app.get("/three_new_entity_id")
def three_new_entity_id():
    conn=get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = '''
        SELECT entity_id, COUNT(*) AS count
        FROM intel_signals
        WHERE priority_level = 99
        GROUP BY entity_id
        ORDER BY count DESC
        LIMIT 3
        '''
        cursor.execute(query)
        results = cursor.fetchall
        return results
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))
    finally:
        conn.close
        cursor.close
        

        
        
