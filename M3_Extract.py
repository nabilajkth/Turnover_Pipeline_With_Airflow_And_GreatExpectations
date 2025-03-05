'''
=================================================
Milestone 3

Nama  : Nabila Sulistiowati
Batch : CODA-003-RMT

Extract menggunakan pyspark
=================================================
'''


import pyspark
from pyspark.sql import SparkSession


def load_data(file_path):
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    data.show(5)
    return data

# Memanggil Fungsi
file_path = "/opt/airflow/dags/P2M3_nabila-sulistiowati_data_raw.csv"
df = load_data(file_path)

# Membaca file CSV menggunakan PySpark
df=spark.read.csv("/opt/airflow/dags/P2M3_nabila-sulistiowati_data_raw.csv")
df.write.csv("/opt/airflow/dags/P2M3_nabila-sulistiowati_IBM.csv")