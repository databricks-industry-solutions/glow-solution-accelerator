# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Ingest VCF into delta
# MAGIC 
# MAGIC We already have the data in delta, so this is notebook is for benchmarking purposes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run notebook(s) to set everything up

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", False)

# COMMAND ----------

spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", True)
spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", 20)
spark.conf.set("io.compression.codecs", "io.projectglow.sql.util.BGZFCodec")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read VCF

# COMMAND ----------

method = 'ingest'
test = 'vcf'
library = 'glow'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

start_time = time.time()

# COMMAND ----------

vcf_df = spark.read.format("vcf").load(output_vcf) 

# COMMAND ----------

vcf_df.write \
      .format("delta") \
      .mode("overwrite") \
      .save(output_delta_tmp)

# COMMAND ----------

# spark.read.format("vcf").load(output_vcf) \
#                         .write \
#                         .format("delta") \
#                         .mode("overwrite") \
#                         .save(output_delta_tmp)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### log runtime

# COMMAND ----------

end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, method, test, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read delta table in and count

# COMMAND ----------

spark.read.format("delta").load(output_delta_tmp).count()
