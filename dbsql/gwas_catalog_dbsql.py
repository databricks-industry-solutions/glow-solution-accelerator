# Databricks notebook source
# MAGIC %md
# MAGIC ## <img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC 
# MAGIC ### Download + ETL GWAS Catalog
# MAGIC 
# MAGIC Associated SQL Analytics Dashboard can be found [here](https://dbc-6dbb94ac-3561.cloud.databricks.com/sql/dashboards/dea998c6-f69b-4b61-8668-ebf5550be2fc-genome-wide-association-study-catalog?o=1625703061153122)
# MAGIC 
# MAGIC Please rerun this notebook before refreshing SQL dashboard

# COMMAND ----------

import pyspark.sql.functions as fx
import os

# COMMAND ----------

def remove_invalid_characters_from_columns(df, character, replacement):
  renamed_df = df.select([fx.col(col).alias(col.replace(character, replacement)) for col in df.columns])
  return renamed_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### set up paths

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
os.environ['user']= user
path = "dbfs:/home/" + user + "/gwas/"
dbutils.fs.mkdirs(path)

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://www.ebi.ac.uk/gwas/api/search/downloads/ancestry

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://www.ebi.ac.uk/gwas/api/search/downloads/alternative

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://www.ebi.ac.uk/gwas/api/search/downloads/full

# COMMAND ----------

# MAGIC %sh
# MAGIC mv alternative /dbfs/$user/gwas/gwas_catalog_alternative.tsv
# MAGIC mv full /dbfs/home/$user/gwas/gwas_catalog_full.tsv
# MAGIC mv ancestry /dbfs/home/$user/gwas/gwas_catalog_ancestry.tsv

# COMMAND ----------

gwas_catalog_df = spark.read.format("csv").option("delimiter", "\t").option("header", True).load(path + "gwas_catalog_full.tsv")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, " ", "_")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, "(", "")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, ")", "")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, "[", "")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, "]", "")
display(gwas_catalog_df)

# COMMAND ----------

gwas_catalog_df.write.format("delta").option("mergeSchema", True).mode("overwrite").save(path + "gwas_catalog_full.delta")

# COMMAND ----------

gwas_catalog_df = spark.read.format("csv").option("delimiter", "\t").option("header", True).load(path + "gwas_catalog_alternative.tsv")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, " ", "_")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, "(", "")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, ")", "")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, "[", "")
gwas_catalog_df = remove_invalid_characters_from_columns(gwas_catalog_df, "]", "")
display(gwas_catalog_df)

# COMMAND ----------

gwas_catalog_df.write.format("delta").option("mergeSchema", True).mode("overwrite").save(path + "gwas_catalog_alternative.delta")

# COMMAND ----------

ancestry_df = spark.read.format("csv").option("delimiter", "\t").option("header", True).load(path + "gwas_catalog_ancestry.tsv")
ancestry_df = remove_invalid_characters_from_columns(ancestry_df, " ", "_")
display(ancestry_df)

# COMMAND ----------

ancestry_df.write.format("delta").mode("overwrite").save(path + "gwas_catalog_ancestry.delta")

# COMMAND ----------

# spark.sql("create database gwas_catalog")

# COMMAND ----------

# spark.sql("create table gwas_catalog.alternative using delta location {}").format(path + "gwas_catalog_alternative.delta")
# spark.sql("create table gwas_catalog.full using delta location {}").format(path + "gwas_catalog_full.delta")
# spark.sql("create table gwas_catalog.ancestry using delta location {}").format(path + "gwas_catalog_ancestry.delta")
