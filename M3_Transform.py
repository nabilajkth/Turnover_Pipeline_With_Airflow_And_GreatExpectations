'''
=================================================
Milestone 3

Nama  : Nabila Sulistiowati
Batch : CODA-003-RMT

Transform Data menggunakan pyspark
=================================================
'''

import pyspark
from pyspark.sql import SparkSession
from tabulate import tabulate
import re # untuk memanipulasi teks

spark = SparkSession.builder.getOrCreate()

# Membaca DataFrame
df = spark.read.csv("/opt/airflow/dags/P2M3_nabila-sulistiowati_IBM.csv", header=True, inferSchema=True)

# Membuat fungsi untuk mengubah nama kolom menjadi snake_case
def to_snake_case(name):
    # Mengubah nama kolom ke format snake_case
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)  # Menambahkan underscore sebelum huruf kapital
    name = name.lower()  # Mengubah semua huruf menjadi huruf kecil
    return name

# Fungsi untuk mengganti semua nama kolom menjadi snake_case menggunakan fungsi to_snake_case pada dataframe
def transform(data):
    # Mendapatkan semua nama kolom
    columns = data.columns

    # Mengubah semua nama kolom menjadi snake_case
    for col in columns:
        new_col_name = to_snake_case(col)
        data = data.withColumnRenamed(col, new_col_name)  # Mengganti nama kolom
    return data

# Transformasi
df = transform(df)

# Menampilkan nama kolom setelah transformasi
print("Nama kolom setelah transformasi:", df.columns)

# Menyimpan DataFrame PySpark sebagai file CSV
df.coalesce(1).write.csv("/opt/airflow/dags/P2M3_nabila-sulistiowati_IBM_cleaned_5.csv", header=True, mode="overwrite")