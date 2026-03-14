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
#os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\ptrus\.jdks\corretto-1.8.0_472'        #  <----- 🔴JAVA PATH🔴

# Set HADOOP_HOME to the folder ABOVE the bin folder
os.environ["HADOOP_HOME"] = r"C:\hadoop"
# Add the bin folder to your System Path
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

#=============================================================#
#Code from below

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Creating the Spark Session
spark = (SparkSession
         .builder
         .appName("E-Commerce Data Ingestion")
         .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
         .getOrCreate())

#Reading the Nine datasets (E-Commerce CSV files) again. This is INGESTION
customers = spark.read.format("CSV").options(header = "true", inferSchema = "true").load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_customers_dataset.csv")
geolocation = spark.read.format("CSV").options(header = "true", inferSchema = "true").load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_geolocation_dataset.csv")
order_items = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_order_items_dataset.csv")
order_payments = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_order_payments_dataset.csv")
order_reviews = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_order_reviews_dataset.csv")
orders = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_orders_dataset.csv")
products = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_products_dataset.csv")
sellers = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\olist_sellers_dataset.csv")
product_category_name_translation = spark.read.format("CSV").options(header = True, inferSchema = True).load(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\raw\product_category_name_translation.csv")



#Now, we shall perform Data Transformations
#Transformation 1: Removing the Duplicates
customers_drop = customers.dropDuplicates()
#customers_drop.show()
geolocation_drop = geolocation.dropDuplicates()
order_items_drop = order_items.dropDuplicates()
order_payments_drop = order_payments.dropDuplicates()
order_reviews_drop = order_reviews.dropDuplicates()
orders_drop = orders.dropDuplicates()
products_drop = products.dropDuplicates()
sellers_drop = sellers.dropDuplicates()
product_category_name_translation_drop = product_category_name_translation.dropDuplicates()

#Transformation 2: Handling Missing Values
#a. Based on the datasets observation, in the "products, order_review, orders" datasets there are missing values.
#We shall replace the missing values with either "Unknown" or "0" by using .fillna()
products_hmv = products.fillna(
    {"product_category_name" : "Unknown",
     "product_name_lenght" : 0,
     "product_description_lenght": 0,
     "product_photos_qty": 0,
     "product_weight_g": 0,
     "product_length_cm": 0,
     "product_height_cm": 0,
     "product_width_cm": 0
     }
)
print("products_hmv by filling missing values")
products_hmv.filter("product_category_name = 'Unknown' AND product_photos_qty = 0").show(5)

order_reviews_hmv = order_reviews.fillna(
    {"review_comment_title": "Unknown",
     "review_comment_message": "Unknown"
     }
)
print("order_reviews_hmv by filling missing values")
order_reviews_hmv.filter("review_comment_title = 'Unknown' OR review_comment_message = 'Unknown'").show(5)

orders_hmv = orders.fillna(
    {"order_approved_at": 0,
     "order_delivered_carrier_date": 0,
     "order_delivered_customer_date": 0
     }
)
print("orders_hmv by filling missing values")
orders_hmv.filter("order_approved_at = 0").show(5)

#Now, before solving the Transformation 3: Creating Aggregated Tables, we shall go with the STAR SCHEMA MODELING
#Let's Create DIMENSION TABLES first.

customer_dim = customers.select(
    "customer_id",
    "customer_city",
    "customer_state"
).dropDuplicates()

product_dim = products.select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
).dropDuplicates()

sellers_dim = sellers.select(
    "seller_id",
    "seller_city",
    "seller_state"
).dropDuplicates()

#From the Orders dataset, we are selecting only date values from the orders purchase column
date_dim = orders.select(
    col("order_purchase_timestamp")
).dropDuplicates()

date_dim = (date_dim.withColumn("day", dayofmonth(col("order_purchase_timestamp")))
            .withColumn("month", month(col("order_purchase_timestamp")))
            .withColumn("year", year(col("order_purchase_timestamp"))))

#Now, let's create the FACT TABLE.
sales_fact = (orders
.join(order_items, "order_id")
.join(order_payments, "order_id")
.select(
    "order_id",
    "customer_id",
    "product_id",
    "seller_id",
    "order_purchase_timestamp",
    "price",
    "freight_value",
    "payment_value"
))

print("Customers Dimension Table")
customer_dim.show(5)
print("Product Dimension Table")
product_dim.show(5)
print("Sellers Dimension Table")
sellers_dim.show(5)
#print("Orders Dimension Table")
#orders_dim.show(5)
print("Date Dimension Table")
date_dim.show(5)
print("Sales FactTable")
sales_fact.show(5)

#Now, Transformation 3:
#Let us create 2 aggregated tables

daily_sales = (sales_fact
.withColumn("order_date", to_date("order_purchase_timestamp"))
    .groupBy("order_date")
    .agg(round(sum("payment_value"),2).alias("daily_revenue"),
         count("order_id").alias("total_orders")
))

print("Daily Sales")
daily_sales.show(5)

top_products = (sales_fact.groupBy("product_id")
                .agg(count("order_id").alias("total_sales"),
                     round(sum("payment_value"), 2).alias("revenue"))
                .orderBy(col("total_sales").desc())
                )
print("Top Products")
top_products.show(5)


#Now, let's store the processed data into the local machine in the PARQUET format. This is LOADING

#print(r"Writing the processed data to C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\processed")
#customer_dim.write.mode("overwrite").parquet(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\processed\customer_dim")
#product_dim.write.mode("overwrite").parquet(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\processed\product_dim")
#sellers_dim.write.mode("overwrite").parquet(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\processed\sellers_dim")
#orders_dim.write.mode("overwrite").parquet(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\processed\orders_dim")
#date_dim.write.mode("overwrite").parquet(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\processed\date_dim")
#sales_fact.write.mode("overwrite").parquet(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\processed\sales_fact")
#daily_sales.write.mode("overwrite").parquet(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\processed\daily_sales")
#top_products.write.mode("overwrite").parquet(r"C:\Users\ptrus\IdeaProjects\data-engineering-project\data_\processed\top_products")
#print("Writing successful!")

#Let's perform some more transformations using SQL
customer_dim.createOrReplaceTempView("customer_dim")
product_dim.createOrReplaceTempView("product_dim")
sellers_dim.createOrReplaceTempView("sellers_dim")
date_dim.createOrReplaceTempView("date_dim")
sales_fact.createOrReplaceTempView("sales_fact")
daily_sales.createOrReplaceTempView("daily_sales")
top_products.createOrReplaceTempView("top_products")

print("The Top 10 Selling Products from the sales_fact fact table are:")
spark.sql("""
          select
              product_id,
              count(order_id) as total_sales
          from sales_fact
          group BY product_id
          ORDER BY total_sales desc
              limit 10;
""").show()

print("The monthly revenue of all the sales:")
spark.sql("""
          select
              year(order_purchase_timestamp) as year_,
              month(order_purchase_timestamp) as month,
              sum(payment_value) AS monthly_rev
          from sales_fact
          group by year_, month
          order by year_, month;
""").show(10)

print("The frequency of the purchases by each customers:")
spark.sql("""
          select
              customer_id,
              count(order_id) AS purchase_count
          from sales_fact
          group by customer_id
          order by purchase_count desc;
""").show(10)

#print()
spark.sql("""
          select
              pd.product_category_name,
              count(sf.order_id) as tot_sales
          from sales_fact as sf
                   join product_dim pd on sf.product_id = pd.product_id
          group by pd.product_category_name
          order by tot_sales DESC
              limit 10;
""").show()


spark.stop()