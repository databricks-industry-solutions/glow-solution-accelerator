# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Download 1000G variant data

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../../0_setup_constants_glow

# COMMAND ----------

# MAGIC %md
# MAGIC ##### create clean directory to run integration test

# COMMAND ----------

dbutils.fs.rm(dbfs_home_path_str + "genomics/standard", recurse=True)
dbutils.fs.mkdirs(dbfs_home_path_str + "genomics/standard")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### download 1000G data for chrom 1, 2, 3, 4, 20, 21 and 22
# MAGIC 
# MAGIC We download the largest chromosome (1), the smallest chromosome (22), and a selection of others such that we can simulate up to 2 MM variants

# COMMAND ----------

# MAGIC %sh
# MAGIC declare -a chroms=("1" "2" "3" "4" "20" "21" "22")
# MAGIC 
# MAGIC for i in "${chroms[@]}"; do wget ftp://hgdownload.cse.ucsc.edu/gbdb/hg19/1000Genomes/phase3/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz; done
# MAGIC 
# MAGIC mkdir -p $vcfs_path_local
# MAGIC 
# MAGIC cp ALL*.genotypes.vcf.gz $vcfs_path_local

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read 1000 Genomes VCF

# COMMAND ----------

vcf = spark.read.format("vcf").load(vcfs_path) \
                              .drop("genotypes") \
                              .where(fx.col("INFO_AF")[0] >= allele_freq_cutoff)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### checkpoint to delta

# COMMAND ----------

vcf.write.mode("overwrite").format("delta").save(output_vcf_delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### optimize the delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE alex_barreto_variant_db.1kg_variants_pvcf USING DELTA Location '{0}'.format(output_vcf_delta)
# MAGIC -- CREATE TABLE alex_barreto_variant_db.1kg_variants_pvcf USING DELTA Location 'dbfs:/home/alex.barreto@databricks.com/genomics/standard/data/delta/1kg_variants_pvcf.delta'
# MAGIC -- OPTIMIZE alex_barreto_variant_db.1kg_variants_pvcf ZORDER BY contigName

# COMMAND ----------

# from delta.tables import *
# vcf_delta_table = DeltaTable.forPath(spark, output_vcf_delta)
# vcf_delta_table.optimize().executeCompaction()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read back in and count data

# COMMAND ----------

display(spark.read.format("delta").load(output_vcf_delta). \
                                   groupBy("contigName").count(). \
                                   sort("contigName"))
