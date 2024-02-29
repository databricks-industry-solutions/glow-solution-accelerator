# Databricks notebook source
# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

from pyspark.sql.functions import *
vcf_path_chr22 = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
bgen_path_chr22 = "/databricks-datasets/genomics/1kg-bgens/1kg_chr22.bgen"
gff_path = "/databricks-datasets/genomics/gffs/GCF_000001405.39_GRCh38.p13_genomic.gff.bgz"
catalog_name = "xomics_gwas"
variant_db_name = f"{catalog_name}.alex_barreto_variant_db"

# COMMAND ----------

# MAGIC %md # Integrating Variants with Annotations using Glow
# MAGIC
# MAGIC Glow addeds support for loading [GFF3](https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md) files, which are commonly used to store annotations on genomic regions. When combined with variant data, genomic annotations provide context for each change in the genome. Does this mutation cause a change in the protein coding sequence of gene? If so, how does it change the protein? Or is the mutation in a low information part of the genome, also known as “junk DNA”. And everything in between. In this step, we demonstrate how to use Glow's APIs to work with annotations from the [RefSeq database](https://www.ncbi.nlm.nih.gov/refseq/) alongside genomic variants from the [1000 Genomes project](https://www.internationalgenome.org/).
# MAGIC
# MAGIC To start, we will download annotations for the GRCh38 human reference genome from RefSeq and define the paths that we will load our data from.

# COMMAND ----------

# from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.dataframe import *


vcf_path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf.gz"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load the GFF file
# MAGIC
# MAGIC Here, we load the GFF file as an Apache Spark dataframe, using [Glow's GFF reader](https://glow.readthedocs.io/en/stable/etl/gff.html). We can then filter down to all annotations that are on `NC_000022.11`, which is the accession for chromosome 22 in the [GRCh38 patch 13 human genome reference build](https://www.ncbi.nlm.nih.gov/nuccore/568815576), curating these for analysis.

# COMMAND ----------

annotations_df = spark.read \
  .format('gff') \
  .load(gff_path) \
  .filter("seqid = 'NC_000022.11'") \
  .alias('annotations_df')

annotations_df.printSchema()

# COMMAND ----------

# MAGIC %md ###### Maintenance of the demo database for idempotency 

# COMMAND ----------

sql(f'DROP SCHEMA IF EXISTS {variant_db_name} CASCADE')

# COMMAND ----------

sql(f'CREATE SCHEMA IF NOT EXISTS {variant_db_name}')

# COMMAND ----------

annotations_df.write.mode('overwrite').saveAsTable(f'{variant_db_name}.annotations_NC_000022_11')

# COMMAND ----------

# MAGIC %md ###### Sanity check our narrow ```NC_000022.11``` annotations table

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations_NC_000022_11

# COMMAND ----------

# MAGIC %md ###### OPTIMIZE the narrow ```NC_000022.11``` annotations table, ZORDERing BY start, end 

# COMMAND ----------

# MAGIC %sql OPTIMIZE xomics_gwas.alex_barreto_variant_db.annotations_NC_000022_11 ZORDER BY (start, end)

# COMMAND ----------

sql(f'SELECT * FROM {variant_db_name}.annotations').display()

# COMMAND ----------

# MAGIC %md ###### OPTIMIZE the broader annotations table, ZORDERing BY start, end 

# COMMAND ----------

spark.sql(f"OPTIMIZE {variant_db_name}.annotations ZORDER BY (start, end)")

# COMMAND ----------

# MAGIC %md ###### Curate VCF data for CHR 22

# COMMAND ----------

chr22_vcf_df = (
  spark.read.format("vcf").load(vcf_path_chr22)
  .withColumn('var_id',F.md5(F.concat('contigname','start','end','referenceAllele')))
  )
chr22_vcf_df.write.format('delta').saveAsTable(f'{variant_db_name}.chr22_vcf')

# COMMAND ----------

# MAGIC %md ###### Sanity check our CHR 22 dataset

# COMMAND ----------

sql(f'select * from {variant_db_name}.chr22_vcf').display()

# COMMAND ----------

chr22_call_stats_df=chr22_vcf_df.select('var_id','contigName','start','end','names',glow.call_summary_stats('genotypes').alias('stats'))

# COMMAND ----------

# MAGIC %md ###### Write our `summary stats` for CHR 22 to Delta Lake

# COMMAND ----------

chr22_call_stats_df.write.format('delta').saveAsTable(f'{variant_db_name}.chr22_call_stats')

# COMMAND ----------

# MAGIC %md ###### Let's sanity check our schemae in our database

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN xomics_gwas.alex_barreto_variant_db

# COMMAND ----------

# MAGIC %md ###### Search for `APOBEC3B` variants among our `NC_000022.11` annotations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM xomics_gwas.alex_barreto_variant_db.annotations_NC_000022_11
# MAGIC WHERE Name LIKE 'APOBEC3B'

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Variants near APOBEC3B - associated with increased risk of myocardial infarction (heart attack) and coronary artery disease. 
# MAGIC ###### APOBEC3B is thought to regulate inflammation in heart disease.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CCS.var_id, CCS.start, CCS.end, A.name, CCS.stats.*
# MAGIC FROM xomics_gwas.alex_barreto_variant_db.chr22_call_stats CCS
# MAGIC JOIN xomics_gwas.alex_barreto_variant_db.annotations_NC_000022_11 A 
# MAGIC WHERE A.Name LIKE 'APOBEC3B'
# MAGIC AND CCS.start > A.start AND CCS.end < A.end
