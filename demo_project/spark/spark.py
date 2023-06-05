from main import send_data

data = send_data()

appointment_keys =  data['appointment'][0].keys()
councillor_keys =  data['councillor'][0].keys()
patient_councillor_keys = data['patient_councillor'][0].keys()
rating_keys =  data['rating'][0].keys()

print(appointment_keys)
print(councillor_keys)
print(patient_councillor_keys)
print(rating_keys)
