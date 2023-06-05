import uvicorn
from fastapi import FastAPI
import requests

app = FastAPI()

@app.get("/")
def get_api_data(url):

    res = requests.get(url)
    return res.json()

# Run the app
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)