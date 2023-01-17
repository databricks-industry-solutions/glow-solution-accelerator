# Databricks notebook source
# MAGIC %md
# MAGIC ### Query variant database
# MAGIC 
# MAGIC 1. point query of specific variants
# MAGIC 2. range query of specific gene

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

import pyspark.sql.functions as fx

# COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", False)

# COMMAND ----------

# spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", True)
# spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", 20)
# spark.conf.set("io.compression.codecs", "io.projectglow.sql.util.BGZFCodec")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1600")

# COMMAND ----------

variants_df = spark.table("{0}.exploded".format(variant_db_name))
display(variants_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### select random genotype

# COMMAND ----------

def get_random_variant(df, seed=0):
  """
  returns a random chromosome, start position and sampleId for querying
  """
  row = df.sample(False, 0.1, seed=seed).limit(1).collect()
  chrom = row[0].contigName
  start = row[0].start
  sampleId = row[0].sampleId
  return chrom, start, sampleId

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Select all columns from row of interest

# COMMAND ----------

chrom, start, sampleId = get_random_variant(variants_df, seed=42)
spark.sql("select * from {0}.exploded where contigName = '{1}' and start == {2} and sampleId = '{3}'".format(variant_db_name, chrom, start, sampleId)).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### only retrieve genotype

# COMMAND ----------

chrom, start, sampleId = get_random_variant(variants_df, seed=84)
spark.sql("select `calls` from {0}.exploded where contigName = '{1}' and start == {2} and sampleId = '{3}'".format(variant_db_name, chrom, start, sampleId)).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Gene based queries

# COMMAND ----------

# MAGIC %md
# MAGIC ##### persist gene coordinates into memory

# COMMAND ----------

genes = spark.sql("select * from {0}.annotations".format(variant_db_name))
genes.createOrReplaceTempView("genes")
spark.table("genes").persist()
spark.table("genes").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### select random gene to query

# COMMAND ----------

genes_overlap_variants_df = genes.hint("range_join", 10). \
                                  join(variants_df, 
                                       (variants_df.contigName == genes.contigName) &
                                       (variants_df.start > genes.start) & 
                                       (variants_df.start <= genes.end), 
                                       "left_semi")

# COMMAND ----------

# genes_overlap_variants_df.write.mode("overwrite").format("delta").save(gene_overlap_variants_delta)

# COMMAND ----------

display(genes_overlap_variants_df)

# COMMAND ----------

def get_random_gene(df, seed=0):
  """
  returns a random gene for querying
  """
  row = df.sample(False, 0.1, seed=seed).limit(1).collect()
  gene = row[0].gene
  return gene

# COMMAND ----------

gene = get_random_gene(genes_overlap_variants_df, seed=126)
gene

# COMMAND ----------

# MAGIC %md
# MAGIC ##### query all variants in gene

# COMMAND ----------

def get_gene_coords(df, gene):
  coords = df.where(fx.col("gene") == gene).collect()[0]
  return coords.contigName, coords.start, coords.end

# COMMAND ----------

sampleId = "532"
chrom, gene_start, gene_end = get_gene_coords(spark.table("genes"), gene)
spark.sql("select * from {0}.exploded where contigName = '{1}' and start >= {2} and end <= {3} and sampleId = '{4}'".format(variant_db_name, chrom, gene_start, gene_end, sampleId)).collect()

# COMMAND ----------


