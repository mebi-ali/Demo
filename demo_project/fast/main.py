from fastapi import FastAPI
import requests
app = FastAPI()

def hello():
    return 'hello'

@app.get("/")
async def get_data():

    url = "https://tasty.p.rapidapi.com/recipes/list"

    headers = {
    	"X-RapidAPI-Key": "b4243423f4mshf9497499cbedf7cp1590cfjsn2115cba68932",
    	"X-RapidAPI-Host": "tasty.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers) 
    response.raise_for_status()
    return response.json()