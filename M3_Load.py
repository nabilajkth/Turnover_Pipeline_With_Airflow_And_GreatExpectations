'''
=================================================
Milestone 3

Nama  : Nabila Sulistiowati
Batch : CODA-003-RMT

Load Dataframe pyspark ke database MongoDB
=================================================
'''

from pymongo import MongoClient
from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .getOrCreate()

# Membaca file CSV ke dalam DataFrame PySpark
csv_file = "/opt/airflow/dags/P2M3_nabila-sulistiowati_IBM_cleaned_5.csv"
df = spark.read.option("header", "true").csv(csv_file)

# Connect ke MongoDB Atlas dengan pymongo
client = MongoClient("mongodb+srv://bilanabils:bilanabils@bela.scj1c.mongodb.net/")
db = client["IBM-Database"] 
collection = db["HR"]  

# Konversi DataFrame PySpark ke format list of dictionary
data_dict = [row.asDict() for row in df.collect()]

# Simpan data ke MongoDB
if data_dict:
    collection.insert_many(data_dict)
    print("Data berhasil disimpan ke MongoDB!")
else:
    print("Tidak ada data yang disimpan.")