import redis
import requests
import json
import uvicorn
from base_logger import logger
from fastapi import FastAPI
from matching import matching_councillors, get_report_id_data

app = FastAPI()

@app.post("/councillors/")

def get_councillors(report_id: int, number_of_doctors: int = 15) -> dict:
    report_category = get_report_id_data(report_id)
    result = matching_councillors(report_category, number_of_doctors)
    return {report_category: result}



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
