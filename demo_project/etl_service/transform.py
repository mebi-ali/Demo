import json
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import  avg, collect_list, struct
from extract import get_data
spark = SparkSession.builder.getOrCreate()

data = get_data()

def extract_data(table_name):
    return spark.createDataFrame(data[table_name])

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
            patient_appointment_rating['councillor_id'],
            councillor['specialization'],
            patient_appointment_rating['value'])

    councillors_avg_ratings = councillor_patient_appointment_rating.groupBy('councillor_id', 'specialization') \
    .agg(avg('value').alias('avg_rating')).orderBy('avg_rating', ascending=False)

    specialized_councillors_with_avg_ratings = councillors_avg_ratings.groupBy("specialization") \
    .agg(collect_list(struct("councillor_id", "avg_rating")).alias("councillor_ids_with_avg_ratings"))

    return specialized_councillors_with_avg_ratings

# def matching_doctors():

#     #### This is part of 2nd service

#     # filtered_df = specialized_councillors_with_avg_rating.filter(specialized_councillors_with_avg_rating.specialization == 'Depression')

#     # exploded_df = filtered_df.select(explode('councillors_id_with_avg_rating').alias('councillor'))

#     # new_df = exploded_df.select('councillor.councillor_id', 'councillor.avg_rating')

#     df.show(3)



if __name__ == "__main__":
    df =joined_data()
    df.show(3)
