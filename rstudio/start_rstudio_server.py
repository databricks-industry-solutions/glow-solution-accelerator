# Databricks notebook source
# MAGIC %fs mkdirs /databricks/init/rstudio

# COMMAND ----------

# MAGIC %fs ls /databricks/init/rstudio

# COMMAND ----------

script = """
#!/bin/bash
set -euxo pipefail
RSTUDIO_BIN="/usr/sbin/rstudio-server"

if [[ ! -f "$RSTUDIO_BIN" && $DB_IS_DRIVER = "TRUE" ]]; then
  rstudio-server restart || true
fi
"""

dbutils.fs.put("/databricks/init/rstudio/rstudio-start.sh", script, True)
