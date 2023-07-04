import uvicorn
from fastapi import FastAPI
from matching import matching_councillors

app = FastAPI()

<<<<<<< HEAD

@app.get("/councillors/{report_id}/")
def get_councillors(report_id: int) -> list[dict]:
=======
@app.get("/councillors/{report_id}/")

def get_councillors(report_id: int, number_of_councillors: int =15) -> list[dict]:
>>>>>>> f1cddabe60ee508a6e5c931f110b118b29f0ebdb
    """
    Retrieve councillors matching the given report_id and number_of_councillors.

    Parameters:
    - report_id (int): The ID of the report to retrieve councillors for.
    - number_of_councillors (int, optional): The number of councillors to match.
        Defaults to 15 if not provided.

    Returns:
    - list[dict]: A list of dictionary containing the retrieved councillors with their avr_rating.
    """
<<<<<<< HEAD
    result = matching_councillors(report_id)
=======
    result = matching_councillors(report_id, number_of_councillors)
>>>>>>> f1cddabe60ee508a6e5c931f110b118b29f0ebdb
    return result

@app.get("/councillors/{report_id}/{number_of_councillors}")

<<<<<<< HEAD
@app.get("/councillors/{report_id}/{number_of_councillors}")
def get_specific_councillors(report_id: int, number_of_councillors: int) -> list[dict]:
=======
def get_councillors(report_id: int, number_of_councillors: int) -> list[dict]:
>>>>>>> f1cddabe60ee508a6e5c931f110b118b29f0ebdb
    """
    Retrieve councillors matching the given report_id and number_of_councillors.

    Parameters:
    - report_id (int): The ID of the report to retrieve councillors for.
    - number_of_councillors (int): The number of councillors to match.

    Returns:
    - list[dict]: A list of dictionary containing the retrieved councillors with their avr_rating.
    """
    result = matching_councillors(report_id, number_of_councillors)
    return result
<<<<<<< HEAD

=======
>>>>>>> f1cddabe60ee508a6e5c931f110b118b29f0ebdb

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)