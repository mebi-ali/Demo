import redis
import requests
import json
import uvicorn
from base_logger import logger
from fastapi import FastAPI
from matching import matching_councillors

app = FastAPI()

@app.get("/councillors/{report_id}/{number_of_doctors}")

def get_councillors(report_id: int, number_of_doctors: int = 15):
    result = matching_councillors(report_id, number_of_doctors)
    return result



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
