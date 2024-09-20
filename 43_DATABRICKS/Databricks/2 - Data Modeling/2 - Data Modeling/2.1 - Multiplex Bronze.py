# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------



# dbutils.fs.ls("dbfs:/mnt/demo_pro/checkpoints/bronze")
# dbutils.fs.rm("dbfs:/mnt/demo-datasets/DE-Pro/bookstore/kafka-raw/01.json")
# display(dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db"))
files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}")
display(files)

# COMMAND ----------

df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(df_raw)




# COMMAND ----------

# spark.table("bronze").printSchema()
# spark.sql("DESCRIBE DETAIL bronze").show(truncate=False)
# from delta.tables import DeltaTable

# try:
#     deltaTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/")
#     print("Table is a Delta table.")
#     # Optionally, inspect the schema or metadata
#     deltaTable.toDF().printSchema()
# except Exception as e:
#     print("Table is not a Delta table or does not exist. Error:", e)


# spark.sql("DROP TABLE IF EXISTS bronze")


# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
  
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .schema(schema)
                        .load(f"{dataset_bookstore}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                  .writeStream
                      .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")
                      .option("mergeSchema", True)
                      .partitionBy("topic", "year_month")
                      .trigger(availableNow=True)
                      .table("bronze"))
    
    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

