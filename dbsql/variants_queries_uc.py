# Databricks notebook source
# MAGIC %md # Variants analysis related to CHF

# COMMAND ----------

from pyspark.sql.functions import *
vcf_path_chr22 = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
vcf_path_chr1 = "/mnt/genomics/tertiary/glow/photon/data/1kg-vcfs-autosomes/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
vcf_path_chr21 = "/mnt/genomics/tertiary/glow/photon/data/1kg-vcfs-autosomes/ALL.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
bgen_path_chr22 = "/databricks-datasets/genomics/1kg-bgens/1kg_chr22.bgen"
bgen_path_chr21 = "/mnt/genomics/tertiary/glow/photon/data/1kg-bgens/1kg_chr21.bgen"
bgen_path_chr1 = "/mnt/genomics/tertiary/glow/photon/data/1kg-bgens/1kg_chr1.bgen"
gff_annotations = "/mnt/genomics/tertiary/glow/photon/data/delta/gff_annotations.delta"
catalog_name = "xomics_gwas"
variant_db_name = f"{catalog_name}.alex_barreto_variant_db"

# COMMAND ----------

# MAGIC %md ## Ingest ```annotations``` from Delta table populated in the ```generate_gff3_annotations``` notebook

# COMMAND ----------

# spark.sql("create table if not exists {0}.annotations using delta location '{1}'".format(variant_db_name, gff_annotations))

# COMMAND ----------

# spark.sql("optimize {0}.annotations zorder by (start, end)".format(variant_db_name))

# COMMAND ----------

vcf_df_chr1 = (spark.read.format("vcf").load(vcf_path_chr1))
display(vcf_df_chr1)

# COMMAND ----------

# vcf_df_chr1 = (spark.read.format("vcf").load(vcf_path_chr1))
# vcf_df_chr1.write.format("delta").saveAsTable("alex_barreto_variant_db.vcfs_autosomes_1kg_chr1")
vcf_df_chr1 = spark.sql("SELECT * FROM alex_barreto_variant_db.vcfs_autosomes_1kg_chr1") 

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- OPTIMIZE alex_barreto_variant_db.vcfs_autosomes_1kg_chr1 ZORDER BY (start, end)

# COMMAND ----------

genes = spark.sql("select * from {0}.annotations".format(variant_db_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations WHERE contigName = '1'

# COMMAND ----------

# MAGIC %md ## Hopefully we have some coverage of genes in our database associated with CHF
# MAGIC
# MAGIC #### ACTN2 (CHR 5), ADRB2, HSPB7 (CHR 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations WHERE gene LIKE "%ACTN2%" OR gene LIKE "ADRB2%" OR gene LIKE "%HSPB7%"

# COMMAND ----------

# MAGIC %md ## Let's query our annotations table for genes in CHR 1 associated with CHF
# MAGIC
# MAGIC #### ADRB2, HSPB7

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations WHERE gene LIKE "ADRB2%" OR gene LIKE "%HSPB7%"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Let's look for SNPs relating to CHF

# COMMAND ----------

# bgen_df_chr1 = spark.read.format("bgen").schema(vcf_df_chr1.schema).load(bgen_path_chr1)
# bgen_df_chr1.write.format("delta").saveAsTable("alex_barreto_variant_db.bgen_1kg_chr1")
bgen_df_chr1 = spark.sql("SELECT * FROM xomics_gwas.alex_barreto_variant_db.bgen_1kg_chr1")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- OPTIMIZE xomics_gwas.alex_barreto_variant_db.bgen_1kg_chr1 ZORDER BY (start, end)

# COMMAND ----------

merged_df_chr1 = vcf_df_chr1.union(bgen_df_chr1)

# COMMAND ----------

chf_variants_df_chr1 = merged_df_chr1.filter((merged_df_chr1.start >= 16014028) & (merged_df_chr1.end <= 16019594)).withColumn("firstGenotype", expr("genotypes[0]")).drop("genotypes")
display(chf_variants_df_chr1)

# COMMAND ----------

# chf_variants_df_chr1.write.format("delta").mode("overwrite").saveAsTable("xomics_gwas.alex_barreto_variant_db.chf_variants_chr1")
chf_variants_df_chr1 = spark.sql("SELECT * FROM xomics_gwas.alex_barreto_variant_db.chf_variants_chr1")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE xomics_gwas.alex_barreto_variant_db.chf_variants_chr1 ZORDER BY (start, end)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.chf_variants_chr1

# COMMAND ----------

# MAGIC %md ## Let's look for annotations relating to these SNPs

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations WHERE contigName = '1'

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations A INNER JOIN xomics_gwas.alex_barreto_variant_db.chf_variants_chr1 V WHERE A.contigName >= V.contigName AND V.START >= 16014028 AND V.END <= 16019594

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations A INNER JOIN xomics_gwas.alex_barreto_variant_db.chf_variants_chr1 V WHERE A.START >= V.START AND A.END >= V.END

# COMMAND ----------

gff_annotations_df_chr1 = spark.table("xomics_gwas.alex_barreto_variant_db.gff_annotations_chr1")

# COMMAND ----------

display(gff_annotations_df_chr1)

# COMMAND ----------

gff_annotations_df_chf = gff_annotations_df_chr1.filter((gff_annotations_df_chr1.start >= 16014028) & (gff_annotations_df_chr1.end <= 16019594))
display(gff_annotations_df_chf)

# COMMAND ----------

# gff_annotations_df_chf.write.format("delta").mode("overwrite").saveAsTable("xomics_gwas.alex_barreto_variant_db.gff_annotations_chr1_chf")
gff_annotations_df_chf = spark.sql("SELECT * FROM xomics_gwas.alex_barreto_variant_db.gff_annotations_chr1_chf")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE xomics_gwas.alex_barreto_variant_db.gff_annotations_chr1_chf ZORDER BY (start, end)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.gff_annotations_chr1_chf A INNER JOIN xomics_gwas.alex_barreto_variant_db.chf_variants_chr1 V WHERE A.START >= V.START AND A.END >= V.END

# COMMAND ----------

# MAGIC %md ### Now we will perform analyses on genes and SNPs in our CHR 22 data

# COMMAND ----------

# MAGIC %md #### Load our relevant CHR22 data

# COMMAND ----------

bgen_df_chr22 = spark.sql("SELECT * FROM xomics_gwas.alex_barreto_variant_db.bgen_1kg_chr22")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- OPTIMIZE xomics_gwas.alex_barreto_variant_db.bgen_1kg_chr22 ZORDER BY (start, end)

# COMMAND ----------

vcf_df_chr22 = spark.sql("SELECT * FROM xomics_gwas.alex_barreto_variant_db.vcfs_autosomes_1kg_chr22")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- OPTIMIZE xomics_gwas.alex_barreto_variant_db.vcfs_autosomes_1kg_chr22 ZORDER BY (start, end)

# COMMAND ----------

merged_df_chr22 = vcf_df_chr22.union(bgen_df_chr22)

# COMMAND ----------

# MAGIC %md ### Let's query our database for Mowat-Wilson Syndrome: gene ```ZEP2``` in CHR 22

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations WHERE gene LIKE "ZEP2%"

# COMMAND ----------

# MAGIC %md ##### No results. We'd want to expand ingestion from our source (e.g. UKBB, DNANexus, gnomAD, GeneBass, TASCA, etc.) and re-process

# COMMAND ----------

# MAGIC %md ### Let's query our database for Phelan-McDermid Syndrome: gene ```SHANK3``` in CHR 22

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations WHERE gene LIKE "SHANK3%"

# COMMAND ----------

# MAGIC %md ##### Great, we have some data on SHANK3, we can proceed

# COMMAND ----------

# MAGIC %md ## Let's look for SNPs relating to Phelan-McDermid Syndrome

# COMMAND ----------

merged_df_chr22.createOrReplaceTempView("chr22_variants")

# COMMAND ----------

pms_variants_df_chr22 = merged_df_chr22.filter((merged_df_chr22.start >= 16014028) & (merged_df_chr22.end <= 16019594)).withColumn("firstGenotype", expr("genotypes[0]")).drop("genotypes")
display(chf_variants_df_chr1)

# COMMAND ----------

# pms_variants_df_chr22.write.format("delta").mode("overwrite").saveAsTable("xomics_gwas.alex_barreto_variant_db.pms_variants_chr22")
pms_variants_df_chr22 = spark.sql("SELECT * FROM xomics_gwas.alex_barreto_variant_db.pms_variants_chr22")

# COMMAND ----------

# MAGIC %md ## Let's look for annotations relating to these SNPs

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.gff_annotations_chr22 WHERE start >= 50117579 AND END <= 50127580

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations A INNER JOIN xomics_gwas.alex_barreto_variant_db.pms_variants_chr22 V WHERE A.contigName >= V.contigName AND V.START >= 51117579 AND V.END <= 51117580

# COMMAND ----------


