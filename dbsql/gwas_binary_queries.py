# Databricks notebook source
import glow

import json
import numpy as np
import pandas as pd
import pyspark.sql.functions as fx

from matplotlib import pyplot as plt
from bioinfokit import visuz

spark = glow.register(spark)

# COMMAND ----------

results_df = spark.sql("SELECT * FROM alex_barreto_gwas_db.gwas_chr1_binary_analysis")

# COMMAND ----------

display(results_df.filter(results_df.phenotype == "Trait_2"))

# COMMAND ----------

pdf = results_df.toPandas()

# COMMAND ----------

visuz.marker.mhat(pdf.loc[pdf.phenotype == 'Trait_2', :], chr='contigName', pv='pvalue', show=True, gwas_sign_line=True)
