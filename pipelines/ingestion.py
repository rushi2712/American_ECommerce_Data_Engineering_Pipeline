import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\ptrus\.jdks\corretto-1.8.0_472'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

#=============================================================#
#Code from below

from pyspark.sql import SparkSession

#Creating the Spark Session
spark = SparkSession.builder.appName("E-Commerce Data Ingestion").getOrCreate()

#Reading NINE E-Commerce CSV files
customers = spark.read.format("CSV").options(header = "true", inferSchema = "true").load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_customers_dataset.csv")
geolocation = spark.read.format("CSV").options(header = "true", inferSchema = "true").load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_geolocation_dataset.csv")
order_items = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_order_items_dataset.csv")
order_payments = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_order_payments_dataset.csv")
order_reviews = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_order_reviews_dataset.csv")
orders = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_orders_dataset.csv")
products = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_products_dataset.csv")
sellers = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_sellers_dataset.csv")
product_category_name_translation = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\product_category_name_translation.csv")

print("\n1. Customers Dataset")
customers.show(5)
print("2. Geolocation Dataset")
geolocation.show(5)
print("3. Order Items Dataset")
order_items.show(5)
print("4. Order Payments Dataset")
order_payments.show(5)
print("5. Order Reviews Dataset")
order_reviews.show(5)
print("6. Orders Dataset")
orders.show(5)
print("7. Products Dataset")
products.show(5)
print("8. Sellers Dataset")
sellers.show(5)
print("9. Translation Dataset")
product_category_name_translation.show(5)
print("Successfully ingested all 9 CSV files")

spark.stop()