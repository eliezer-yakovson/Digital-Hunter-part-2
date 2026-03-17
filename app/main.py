from fastapi import FastAPI, HTTPException
import mysql.connector
import os
import math

app = FastAPI()

def get_db_connection():
    return mysql.connector.connect(
        host = os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT", "3311")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "root"),
        database=os.getenv("DB_NAME", "digital_hunter")
    )
    
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance in km between two points on Earth."""
    EARTH_RADIUS_KM = 6371.0

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return EARTH_RADIUS_KM * c

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
        results = cursor.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail= str(e))
    finally:
        cursor.close()
        conn.close()
        
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
        results = cursor.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail= str(e))
    finally:
        cursor.close()
        conn.close()
        
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
        results = cursor.fetchall()
        return results
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))
    finally:
        conn.close()
        cursor.close()


@app.get("/targets/awakened-sleepers")
def get_awakened_sleeper_cells():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = """
            WITH DayStaticTargets AS (
                    SELECT entity_id, DATE(timestamp) AS static_date
                    FROM intel_signals
                    WHERE TIME(timestamp) >= '08:00:00' AND TIME(timestamp) < '20:00:00'
                    GROUP BY entity_id, DATE(timestamp)
                    HAVING MAX(reported_lat) = MIN(reported_lat) 
                       AND MAX(reported_lon) = MIN(reported_lon)
                ),
                NightBounds AS (
                    SELECT 
                        entity_id,
                        DATE(timestamp - INTERVAL 12 HOUR) AS logical_date,
                        SUM(distance_from_last) AS total_night_distance
                    FROM intel_signals
                    WHERE TIME(timestamp) >= '20:00:00' OR TIME(timestamp) < '08:00:00'
                    GROUP BY entity_id, DATE(timestamp - INTERVAL 12 HOUR)
                    HAVING SUM(distance_from_last)>=10
                )
                SELECT d.entity_id, n.total_night_distance
                FROM DayStaticTargets d
                JOIN NightBounds n 
                  ON d.entity_id = n.entity_id 
                 AND d.static_date = n.logical_date
            """
        cursor.execute(query)
        results = cursor.fetchall()          
        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)