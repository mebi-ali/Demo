import requests
import os
import sys
sys.path.insert(0, '.')
from fast.main import get_api_data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# # Define the schema
# schema = StructType([
#     StructField("name", StringType(), nullable=False),
#     StructField("age", IntegerType(), nullable=True),
#     StructField("city", StringType(), nullable=True)
# ])

# Create an example DataFrame with the defined schema




appointment = councillor = patient_councillor = rating = None


urls =  {
    "appointment" : "https://xloop-dummy.herokuapp.com/appointment",
    "councillor" : "https://xloop-dummy.herokuapp.com/councillor", 
    "patient_councillor" : "https://xloop-dummy.herokuapp.com/patient_councillor",
    "rating" : "https://xloop-dummy.herokuapp.com/rating"
}

appointment = get_api_data(urls['appointment'])
councillor = get_api_data(urls['councillor'])
patient_councillor = get_api_data(urls['patient_councillor'])
rating = get_api_data(urls['rating'])



keys = rating[0].keys()
print(keys)

df = spark.createDataFrame(appointment, schema=keys)
# Show the DataFrame
df.show()


