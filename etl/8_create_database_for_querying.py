# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Create database for querying
# MAGIC 
# MAGIC use Spark SQL to create database and tables within that database for downstream querying

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %md
# MAGIC ##### create database for querying

# COMMAND ----------

spark.sql("create database if not exists {}".format(variant_db_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### add variants to database

# COMMAND ----------

spark.sql("drop table if exists variant_db.exploded")
spark.sql("drop table if exists variant_db.annotations")
spark.sql("drop table if exists variant_db.pvcf")

# COMMAND ----------

spark.sql("create table {0}.exploded using delta location '{1}'".format(variant_db_name, output_exploded_delta))

# COMMAND ----------

spark.sql("create table {0}.annotations using delta location '{1}'".format(variant_db_name, gff_annotations))

# COMMAND ----------

spark.sql("create table {0}.pvcf using delta location '{1}'".format(variant_db_name, output_simulated_delta))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### optimize delta lake table for querying
# MAGIC 
# MAGIC by optimizing file sizes and Z-ordering on `contigName` and `start`.

# COMMAND ----------

display(spark.sql("OPTIMIZE variant_db.exploded ZORDER BY (contigName, start)"))

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY {0}.exploded".format(variant_db_name)))
