# Databricks notebook source
import os

current_directory = os.getcwd()
print({current_directory})

bronze_path = f"{current_directory}/bronze/"
silver_path = f"{current_directory}/silver/"
gold_path = f"{current_directory}/gold/"

# dbutils.fs.rm(bronze_path, True)
# dbutils.fs.rm(silver_path, True)
# dbutils.fs.rm(gold_path, True)

dbutils.fs.mkdirs(bronze_path)
dbutils.fs.mkdirs(silver_path)
dbutils.fs.mkdirs(gold_path)

display(dbutils.fs.ls(current_directory))

# COMMAND ----------

import requests
import json
import os

response = requests.get("https://api.openbrewerydb.org/breweries")
breweries_data = response.json()
breweries_data_str = json.dumps(breweries_data)

directory_bronze = bronze_path
file_path_bronze = os.path.join(directory_bronze, 'breweries.json')
dbutils.fs.put(file_path_bronze, breweries_data_str, overwrite=True)
# display(dbutils.fs.ls(file_path_bronze))

print(f"Base carregada com sucesso. Listagem de arquivos do Bronze: {dbutils.fs.ls(directory_bronze)}")

print("Preview dos dados:")
display(spark.read.json(file_path_bronze))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DecimalType
from pyspark.sql.functions import current_timestamp, col, hash, concat_ws, when, count
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

directory_silver = silver_path
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("monitoring")

spark = SparkSession.builder \
    .appName("CaseInBev_DataTransformation") \
    .getOrCreate()

try:
    df = spark.read.json(file_path_bronze)
    # df.printSchema()

    df = df.withColumn("latitude", col("latitude").cast(DecimalType(10, 8))) \
            .withColumn("longitude", col("longitude").cast(DecimalType(11, 8))) \
            .withColumn("timestamp", current_timestamp())

    df_clean = df.filter(df['state'].isNotNull())

    df_clean = df_clean.withColumn("id_hash", hash(concat_ws("_", "latitude", "longitude", "state", "city")))
    duplicate_rows = df_clean.groupBy("id_hash").count().filter("count > 1")
    print("Duplicate values:")
    display(duplicate_rows)

    df_clean = df_clean.dropDuplicates(["id_hash"])

    print("Null values:")
    df_nulls = df_clean.select([count(when(col(c).isNull(), c)).alias(c) for c in ["latitude", "longitude", "state"]])
    display(df_nulls)

    logger.info("Data successfully.")
    display(dbutils.fs.ls(directory_silver))
    
except Exception as e:
    logger.error(f"Failure: {e}")

# COMMAND ----------

# dbutils.fs.ls(f"{directory_silver}/state=Indiana/")
# path_indiana = f"{directory_silver}/state=Indiana/"
# df_indiana = spark.read.parquet(path_indiana)
# display(df_indiana)

# path_oregon = f"{directory_silver}/state=Oregon/"
# df_oregon = spark.read.parquet(path_oregon)
# display(df_oregon)

# path_maryland = f"{directory_silver}/state=Maryland/"
# df_maryland = spark.read.parquet(path_maryland)
# display(df_maryland)


# COMMAND ----------

import pandas as pd
import plotly.express as px

directory_gold = gold_path

df_clean = df_clean.withColumn("latitude", col("latitude").cast("float")) \
                   .withColumn("longitude", col("longitude").cast("float"))

pandas_df = df_clean.select("latitude", "longitude", "state", "brewery_type").toPandas()
pandas_df['size'] = 10

fig = px.scatter_mapbox(
    pandas_df,
    lat="latitude",
    lon="longitude",
    hover_name="state",
    hover_data={"brewery_type": True, "size": False},
    size="size",
    zoom=3,
    height=500,
    width=800
)

fig.update_layout(mapbox_style="open-street-map")
fig.show()

aggregate_df = df_clean.groupBy("state", "brewery_type").count()
aggregate_df = aggregate_df.orderBy("state", "brewery_type")
aggregate_df.show(n=aggregate_df.count(), truncate=False)
aggregate_df.write.mode("overwrite").parquet(directory_gold)

df_gold = spark.read.parquet(directory_gold)
df_gold.show(n=df_gold.count(), truncate=False)
df_gold.select([count(when(col(c).isNull(), c)).alias(c) for c in df_gold.columns]).show()
