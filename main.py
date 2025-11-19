from fastapi import FastAPI
from analytics_polars import get_summary, get_trend
from fastapi.responses import HTMLResponse

from create_data import create_data

create_data()

from build_data import task_1,log_errors,log_clean,create_connection

app = FastAPI(title="Trade Analytics API (Polars Version)")

# === Serve index.html ===
@app.get("/", response_class=HTMLResponse)
def home():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/summary")
def summary():
    return get_summary()

@app.get("/api/trend/{symbol}")
def trend(symbol: str):
    return get_trend(symbol)

if __name__ == "__main__":
    create_connection()

    # Prints answers for task 1
    task_1()

    # prints errors in raw dataset
    log_errors()

    # prints cleaned dataset
    log_clean()

    '''import uvicorn
    uvicorn.run("main:app", host= "127.0.0.1", port=8000, reload=True) # reload=True''' #- gets page stuck in infinite loading

    #uvicorn can be run in console with: 'uvicorn "main:app" --reload' without errors