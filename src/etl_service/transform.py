import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import  avg
from pyspark.sql.utils import AnalysisException
from extract import get_api_data, urls
from base_logger import logger


def get_all_tables(spark: SparkSession) -> dict:
    dataframes = {key: spark.createDataFrame(get_api_data(url)) for key, url in urls.items()}
    logger.info("Data received from endpoints")
    return dataframes    

def joined_data(spark: SparkSession) -> dict: 
    dataframes = get_all_tables(spark)
    appointment = dataframes['appointment']
    councillor = dataframes['councillor']
    patient_councillor = dataframes['patient_councillor']
    rating = dataframes['rating']

    councillor_specialization_rating = (
            appointment.join(
                patient_councillor,
                appointment["patient_id"] == patient_councillor["patient_id"],
            )
            .join(rating, appointment["id"] == rating["appointment_id"])
            .join(
                councillor, councillor["id"] == patient_councillor["councillor_id"]
            )
            .select(
                councillor["id"].alias("councillor_id"),
                councillor["specialization"],
                rating["value"],
            )
        )
    return councillor_specialization_rating
    # councillors_avg_ratings = councillor_specialization_rating.groupBy('councillor_id', 'specialization') \
    # .agg(avg('value').alias('avg_rating')).orderBy('avg_rating', ascending=False)
    # logger.info("Data has been Joined.")
    # return councillors_avg_ratings

def transformations() -> dict:
    spark = SparkSession.builder.appName("TransformationPhase").getOrCreate()
    councillor_specialization_rating = joined_data(spark)
    specializations = [row.specialization for row in  
                        councillor_specialization_rating.select("specialization").distinct().collect()]
    specializations_dfs = {}
    for specialization in specializations:
        specializations_dfs[specialization] = councillor_specialization_rating.filter(
            councillor_specialization_rating["specialization"] == specialization) \
            .groupBy("councillor_id").agg(avg("value").alias("average_rating")).orderBy("average_rating", ascending=False) \
            .drop("specialization").toJSON().collect()
    spark.stop()
    logger.info("Data has been transformed.")
    return specializations_dfs

if __name__ == "__main__":
    transformations()
