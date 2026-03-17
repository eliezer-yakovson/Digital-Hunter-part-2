from fastapi import FastAPI, HTTPException
from DigitalHunter_map import plot_map_with_geometry
import db_queries

app = FastAPI()

@app.get("/movement_alert")
def movement_alert():
    try:
        return db_queries.fetch_movement_alerts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/signal_type_sorted")
def signal_type_sorted():
    try:
        return db_queries.fetch_signal_type_sorted()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/three_new_entity_id")
def three_new_entity_id():
    try:
        return db_queries.fetch_three_new_entity_id()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/targets/awakened-sleepers")
def get_awakened_sleeper_cells():
    try:
        return db_queries.fetch_awakened_sleeper_cells()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/coordinate_motion_graph/{id}")
def coordinate_motion_graph(id: str):
    try:
        results = db_queries.fetch_coordinate_motion(id)
        
        coords_list = [(row['reported_lon'], row['reported_lat']) for row in results]
        plot_map_with_geometry(coords_list)
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)