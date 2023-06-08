import json
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql import SQLContext
from pyspark.sql.functions import collect_set, avg
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from extract import main


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
            councillor['specialization'],
            patient_appointment_rating['value'])

    return councillor_patient_appointment_rating

def matching_doctors():
    # councillor_patient_appointment_rating = joined_data()
    df = joined_data()
    # councillors_avg_ratings = councillor_patient_appointment_rating.groupBy('councillor_id').avg('value').alias('avg_rating')
    
    # # councillor_patient_appointment_rating = councillor_patient_appointment_rating.join(councillors_avg_ratings, "councillor_id")
    
#     councillor_all_avarege_on_specialization = councillor_patient_appointment_rating.groupBy('specialization').agg(collect_set('councillor_id').alias('unique_councillors'), avg('value').alias('avg_rating'))
    
#     # Calculate the average rating for each counselor within each specialization
#     # avg_rating_df = councillor_patient_appointmsent_rating.groupBy('specialization', 'councillor_id').agg(avg('value').alias('avg_rating'))

# # Show the result
#     councillor_all_avarege_on_specialization.show(3)

#     windowSpec = Window.partitionBy("specialization", "councillor_id")

# # Calculate the average rating for each counselor based on distinct specializations
#     df = df.withColumn("avg_rating", F.avg("value").over(windowSpec))

# # Group by specialization and collect the unique counselors with their average rating
#     df = df.groupby("specialization").agg(F.collect_list(F.struct("councillor_id", "avg_rating")).alias("unique_councillors_with_their_avg_rating"))

# # Show the resulting DataFrame
#     df.show(5)

# Assuming your Spark DataFrame is named 'df'
    # df = df.groupBy("specialization", "councillor_id").agg(F.avg("value").alias("avg_rating"))
    # df = df.groupBy("specialization").agg(F.collect_list(F.expr("named_struct('councillor', councillor_id, 'avg_rating', avg_rating)")).alias("unique_councillors_with_their_avg_rating"))

# Show the resulting DataFrame


    
    return
    


if __name__ == "__main__":
    data = main()
    matching_doctors()