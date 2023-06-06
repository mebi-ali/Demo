import json
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from main import send_data

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

data = send_data()

appointment = spark.read.json(spark.sparkContext.parallelize([json.dumps(data["appointment"])]))
councillor = spark.read.json(spark.sparkContext.parallelize([json.dumps(data['councillor'])]))
patient_councillor = spark.read.json(spark.sparkContext.parallelize([json.dumps(data['patient_councillor'])]))
rating = spark.read.json(spark.sparkContext.parallelize([json.dumps(data['rating'])]))


appointment_rating = appointment.join(rating, appointment['id']==rating['appointment_id']).select(appointment['patient_id'], rating['value'])
patient_appointment_rating = appointment_rating.join(patient_councillor, appointment_rating['patient_id']==patient_councillor['patient_id'])

patient_appointment_rating.show(2)