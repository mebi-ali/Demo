import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  avg, collect_list, struct
from extract import get_api_data
from base_logger import logger
from dotenv import load_dotenv
load_dotenv()


def get_all_tables(spark: SparkSession.builder.appName) -> tuple:
    
    appointment = spark.createDataFrame(get_api_data(os.environ.get("APPOINTMENT"))) 
    councillor = spark.createDataFrame(get_api_data(os.environ.get("COUNCILLOR")))
    patient_councillor = spark.createDataFrame(get_api_data(os.environ.get("PATIENT_COUNCILLOR"))) 
    rating = spark.createDataFrame(get_api_data(os.environ.get("RATING")))
    logger.info("Data received from endpoints")
    return appointment, councillor, patient_councillor, rating

def joined_data() -> dict: 
    spark = SparkSession.builder.appName("TransformationPhase").getOrCreate()

    appointment, councillor, patient_councillor,rating = get_all_tables(spark)

    appointment_rating = appointment.join(rating, appointment['id']==rating['appointment_id']) \
    .select(appointment['patient_id'], rating['value'])

    patient_appointment_rating = appointment_rating.join(patient_councillor, 
        appointment_rating['patient_id']==patient_councillor['patient_id']) \
        .select(
            appointment_rating['patient_id'],
            patient_councillor['councillor_id'], 
            appointment_rating['value'])

    councillor_specialization_rating = patient_appointment_rating.join(councillor,
        patient_appointment_rating['councillor_id']==councillor['id']) \
        .select(
            patient_appointment_rating['councillor_id'],
            councillor['specialization'],
            patient_appointment_rating['value'])

    councillors_avg_ratings = councillor_specialization_rating.groupBy('councillor_id', 'specialization') \
    .agg(avg('value').alias('avg_rating')).orderBy('avg_rating', ascending=False)

    
    specializations = [row.specialization for row in  
                        councillor_specialization_rating.select("specialization").distinct().collect()]

    specializations_dfs = {}
    for specialization in specializations:
        specializations_dfs[specialization] = councillors_avg_ratings.filter(
        councillors_avg_ratings.specialization == specialization).drop("specialization").toJSON().collect()
    spark.stop()

    logger.info("Data has been transformed")
    return specializations_dfs


if __name__ == "__main__":
    joined_data()