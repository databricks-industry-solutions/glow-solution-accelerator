# Databricks notebook source
# MAGIC %md # Variants analysis related to CHF

# COMMAND ----------

from pyspark.sql.functions import *
vcf_path_chr22 = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
vcf_path_chr1 = "/mnt/genomics/tertiary/glow/photon/data/1kg-vcfs-autosomes/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
vcf_path_chr21 = "/mnt/genomics/tertiary/glow/photon/data/1kg-vcfs-autosomes/ALL.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
bgen_path_chr22 = "/databricks-datasets/genomics/1kg-bgens/1kg_chr22.bgen"
bgen_path_chr21 = "/databricks-datasets/genomics/1kg-bgens/1kg_chr22.bgen"
bgen_path_chr1 = "/databricks-datasets/genomics/1kg-bgens/1kg_chr22.bgen"

# COMMAND ----------

vcf_df_chr1 = (spark.read.format("vcf").load(vcf_path_chr1))
display(vcf_df_chr1)

# COMMAND ----------

vcf_df_chr1 = (spark.read.format("vcf").load(vcf_path_chr1))
vcf_df_chr1.write.format("delta").saveAsTable("alex_barreto_variant_db.vcfs_autosomes_1kg_chr1") 

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_variant_db.vcfs_autosomes_1kg_chr1 ZORDER BY (start, end)

# COMMAND ----------

vcf_df_chr21 = (spark.read.format("vcf")
  .load(vcf_path_chr21))
display(vcf_df_chr21)

# COMMAND ----------

vcf_df_chr21 = (spark.read.format("vcf").load(vcf_path_chr21))
vcf_df_chr21.write.format("delta").saveAsTable("alex_barreto_variant_db.vcfs_autosomes_1kg_chr21")

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_variant_db.vcfs_autosomes_1kg_chr21 ZORDER BY (start, end)

# COMMAND ----------

vcf_df_chr22 = (spark.read.format("vcf")
  .load(vcf_path_chr22))
display(vcf_df_chr22)

# COMMAND ----------

vcf_df_chr22 = (spark.read.format("vcf").load(vcf_path_chr22))
vcf_df_chr22.write.format("delta").saveAsTable("alex_barreto_variant_db.vcfs_autosomes_1kg_chr22")

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_variant_db.vcfs_autosomes_1kg_chr22 ZORDER BY (start, end)

# COMMAND ----------

df = spark.read.format("bgen").load(bgen_path_chr_22)
display(df)

# COMMAND ----------

variant_db_name="alex_barreto_variant_db"
genes = spark.sql("select * from {0}.annotations".format(variant_db_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM alex_barreto_variant_db.annotations

# COMMAND ----------

# MAGIC %md ## Hopefully we have some coverage of genes in our database associated with CHF
# MAGIC
# MAGIC #### ACTN2 (CHR 5), ADRB2, HSPB7 (CHR 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM alex_barreto_variant_db.annotations WHERE gene LIKE "%ACTN2%" OR gene LIKE "ADRB2%" OR gene LIKE "%HSPB7%"

# COMMAND ----------

# MAGIC %md ## Let's query our annotations table for genes in CHR 1 associated with CHF
# MAGIC
# MAGIC #### ADRB2, HSPB7

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM alex_barreto_variant_db.annotations WHERE gene LIKE "ADRB2%" OR gene LIKE "%HSPB7%"

# COMMAND ----------

# MAGIC %md ## Let's look for SNPs relating to CHF

# COMMAND ----------

bgen_df_chr22 = spark.read.format("bgen").schema(vcf_df_chr22.schema).load(bgen_path_chr22)
bgen_df_chr22.write.format("delta").saveAsTable("alex_barreto_variant_db.bgen_1kg_chr22")


# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_variant_db.bgen_1kg_chr22 ZORDER BY (start, end)

# COMMAND ----------

merged_df_chr22 = vcf_df_chr22.union(bgen_df_chr22)

# COMMAND ----------

bgen_df_chr21 = spark.read.format("bgen").schema(vcf_df_chr21.schema).load(bgen_path_chr21)
bgen_df_chr21.write.format("delta").saveAsTable("alex_barreto_variant_db.bgen_1kg_chr21")


# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_variant_db.bgen_1kg_chr21 ZORDER BY (start, end)

# COMMAND ----------

merged_df_chr21 = vcf_df_chr21.union(bgen_df_chr21)

# COMMAND ----------

bgen_df_chr1 = spark.read.format("bgen").schema(vcf_df_chr1.schema).load(bgen_path_chr1)
bgen_df_chr1.write.format("delta").saveAsTable("alex_barreto_variant_db.bgen_1kg_chr1")

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE alex_barreto_variant_db.bgen_1kg_chr1 ZORDER BY (start, end)

# COMMAND ----------

merged_df_chr1 = vcf_df_chr1.union(bgen_df_chr1)

# COMMAND ----------

# sanity check

display(merged_df_chr1.filter((merged_df_chr1.start >= 16014028) & (merged_df_chr1.end <= 16019594)).withColumn("firstGenotype", expr("genotypes[0]")).drop("genotypes").limit(10))

# COMMAND ----------

display(merged_df_chr1.filter((merged_df_chr1.start >= 16014028) & (merged_df_chr1.end <= 16019594)).withColumn("firstGenotype", expr("genotypes[0]")).drop("genotypes").limit(10))

# COMMAND ----------

chf_variants_df_chr1 = merged_df_chr1.filter((merged_df_chr1.start >= 16014028) & (merged_df_chr1.end <= 16019594)).withColumn("firstGenotype", expr("genotypes[0]")).drop("genotypes")
display(chf_variants_df_chr1)

# COMMAND ----------

# genes = spark.sql("select * from {0}.annotations".format(variant_db_name))
# chf_variants_df.write.format("delta").saveAsTable("{0}.chf_variants")
chf_variants_df_chr1.write.format("delta").saveAsTable("alex_barreto_variant_db.chf_variants_chr1")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE ${0}.chf_variants ZORDER BY gene, ID, seqId
# MAGIC OPTIMIZE alex_barreto_variant_db.chf_variants_chr1 ZORDER BY (start, end)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM ${0}.chf_variants
# MAGIC SELECT * FROM alex_barreto_variant_db.chf_variants_chr1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM ${0}.chf_variants
# MAGIC SELECT * FROM alex_barreto_variant_db.chf_variants_chr1
