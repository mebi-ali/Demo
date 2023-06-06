import json
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql import SQLContext
from get_all_data import main


def extract_data(table_name):
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(data[table_name])]))

def get_all_tables():
    appointment = extract_data('appointment') 
    councillor = extract_data('councillor')
    patient_councillor = extract_data('patient_councillor') 
    rating = extract_data('rating')
    return appointment, councillor, patient_councillor, rating

def joined_data():  
    appointment, councillor, patient_councillor,rating = get_all_tables()

    appointment_rating = appointment.join(rating, appointment['id']==rating['appointment_id']) \
    .select(appointment['patient_id'], rating['value'])

    patient_appointment_rating = appointment_rating.join(patient_councillor, 
        appointment_rating['patient_id']==patient_councillor['patient_id']) \
        .select(
            appointment_rating['patient_id'],
            patient_councillor['councillor_id'], 
            appointment_rating['value'])
        

    councillor_patient_appointment_rating = patient_appointment_rating.join(councillor,
        patient_appointment_rating['councillor_id']==councillor['id']) \
        .select(
            patient_appointment_rating['patient_id'],
            patient_appointment_rating['councillor_id'],
            patient_appointment_rating['value'])

    return councillor_patient_appointment_rating



if __name__ == "__main__":
    data = main()
    df = joined_data()
    df.show(3)