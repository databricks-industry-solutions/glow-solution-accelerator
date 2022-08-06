# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to create a Workflow DAG and illustrate the order of execution. Feel free to interactively run notebooks with the cluster or to run the Workflow to see how this solution accelerator executes. Happy exploring!
# MAGIC 
# MAGIC The pipelines, workflows and clusters created in this script are not user-specific, so if any user alters the workflow and cluster via UI, running this script resets the changes.
# MAGIC 
# MAGIC **Note**: If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators sometimes require the user to set up additional cloud infra or data access, for instance. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-industry-solutions/notebook-solution-companion git+https://github.com/databricks-academy/dbacademy-rest git+https://github.com/databricks-academy/dbacademy-gems 

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

from dbacademy.dbgems import get_username
docker_username = dbutils.secrets.get("solution-accelerator-cicd", "docker_username") # this secret scope is set up to enable testing only in Databricks' internal environment; please set up secret scope with your own credential
docker_password = dbutils.secrets.get("solution-accelerator-cicd", "docker_password") # this secret scope is set up to enable testing only in Databricks' internal environment; please set up secret scope with your own credential
job_json = {
        "timeout_seconds": 0,
        "tags":{
          "usage": "solacc_testing",
          "group": "HLS"
        },
        "email_notifications": {},
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/etl/data/download_1000G"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "download_1000G",
                "description": ""
            },
            {
                "job_cluster_key": "single_node",
                "notebook_task": {
                    "notebook_path": f"/etl/1_simulate_covariates_phenotypes_offset"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "simulate_covariates_phenotypes_offset",
                "depends_on": [
                    {
                        "task_key": "download_1000G"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/etl/2_simulate_delta_pvcf"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "simulate_delta_pvcf",
                "depends_on": [
                    {
                        "task_key": "download_1000G"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/etl/3_delta_to_vcf"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "delta_to_vcf",
                "depends_on": [
                    {
                        "task_key": "simulate_delta_pvcf"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/tertiary/pipe-transformer-plink"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "pipe_transformer_plink",
                "depends_on": [
                    {
                        "task_key": "delta_to_vcf"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/etl/10_liftOver"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "liftOver",
                "depends_on": [
                    {
                        "task_key": "delta_to_vcf"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/tertiary/parallel_bcftools_filter"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "parallel_bcftools_filter",
                "depends_on": [
                    {
                        "task_key": "liftOver"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/tertiary/0_ingest_vcf2delta"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "ingest_vcf2delta",
                "depends_on": [
                    {
                        "task_key": "delta_to_vcf"
                    }
                ]
            },
            {
                "job_cluster_key": "hail",
                "libraries": [
                    {
                        "pypi": {
                            "package": "glow.py==1.2.1"
                        }
                    },
                    {
                        "maven": {
                            "coordinates": "io.projectglow:glow-spark3_2.12:1.2.1"
                        }
                    }
                ],
                "notebook_task": {
                    "notebook_path": f"/etl/4_vcf_to_hail_mt"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "ingest_to_hail",
                "depends_on": [
                    {
                        "task_key": "delta_to_vcf"
                    }
                ]
            },
            {
                "job_cluster_key": "hail_to_glow",
                "libraries": [
                    {
                        "pypi": {
                            "package": "glow.py==1.2.1"
                        }
                    },
                    {
                        "maven": {
                            "coordinates": "io.projectglow:glow-spark3_2.12:1.2.1"
                        }
                    }
                ],
                "notebook_task": {
                    "notebook_path": f"/etl/5_hail_mt_to_glow"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "export_hail_to_glow",
                "depends_on": [
                    {
                        "task_key": "ingest_to_hail"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/tertiary/1_quality_control"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "quality_control",
                "depends_on": [
                    {
                        "task_key": "ingest_vcf2delta"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/etl/6_explode_variant_dataframe"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "explode_variants_for_querying",
                "depends_on": [
                    {
                        "task_key": "quality_control"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/etl/7_etl_gff_annotations"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "generate_gff3_annotations"
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/etl/8_create_database_for_querying"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "create_database_for_querying",
                "depends_on": [
                    {
                        "task_key": "generate_gff3_annotations"
                    },
                    {
                        "task_key": "explode_variants_for_querying"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/etl/9_query_variant_db"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "query_variant_database",
                "depends_on": [
                    {
                        "task_key": "create_database_for_querying"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/tertiary/2_quantitative_glowgr"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "glowgr_quantitative",
                "depends_on": [
                    {
                        "task_key": "simulate_covariates_phenotypes_offset"
                    },
                    {
                        "task_key": "quality_control"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/tertiary/3_linear_gwas_glow",
                    "base_parameters": {
                        "user": get_username() # to pass user email into R
                    }
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "glow_linear_gwas",
                "depends_on": [
                    {
                        "task_key": "glowgr_quantitative"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/tertiary/4_binary_glowgr"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "glowgr_binary",
                "depends_on": [
                    {
                        "task_key": "glow_linear_gwas"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/tertiary/5_logistic_gwas_glow"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "glow_logistic_gwas",
                "depends_on": [
                    {
                        "task_key": "glowgr_binary"
                    }
                ]
            },
            {
                "job_cluster_key": "hail",
                "libraries": [
                    {
                        "pypi": {
                            "package": "glow.py==1.2.1"
                        }
                    },
                    {
                        "maven": {
                            "coordinates": "io.projectglow:glow-spark3_2.12:1.2.1"
                        }
                    }
                ],
                "notebook_task": {
                    "notebook_path": f"/tertiary/6_hail_linreg_gwas"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "hail_linear_gwas",
                "depends_on": [
                    {
                        "task_key": "simulate_covariates_phenotypes_offset"
                    },
                    {
                        "task_key": "ingest_to_hail"
                    }
                ]
            },
            {
                "job_cluster_key": "hail",
                "libraries": [
                    {
                        "maven": {
                            "coordinates": "io.projectglow:glow-spark3_2.12:1.2.1"
                        }
                    },
                    {
                        "pypi": {
                            "package": "glow.py==1.2.1"
                        }
                    }
                ],
                "notebook_task": {
                    "notebook_path": f"/tertiary/7_hail_logistic_gwas"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "hail_logistic_gwas",
                "depends_on": [
                    {
                        "task_key": "simulate_covariates_phenotypes_offset"
                    },
                    {
                        "task_key": "ingest_to_hail"
                    }
                ]
            },
            {
                "job_cluster_key": "hail",
                "libraries": [
                    {
                        "pypi": {
                            "package": "glow.py==1.2.1"
                        }
                    },
                    {
                        "maven": {
                            "coordinates": "io.projectglow:glow-spark3_2.12:1.2.1"
                        }
                    }
                ],
                "notebook_task": {
                    "notebook_path": f"/tertiary/9_compare_hail_to_glow"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "compare_hail_to_glow",
                "depends_on": [
                    {
                        "task_key": "glow_logistic_gwas"
                    },
                    {
                        "task_key": "hail_linear_gwas"
                    },
                    {
                        "task_key": "hail_logistic_gwas"
                    }
                ]
            },
            {
                "job_cluster_key": "glow_integration_test",
                "notebook_task": {
                    "notebook_path": f"/tertiary/8_pipeline_runs_comparison"
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "task_key": "pipeline_benchmarks",
                "depends_on": [
                    {
                        "task_key": "compare_hail_to_glow"
                    }
                ]
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "glow_integration_test",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "10.4.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_D3_v2", "GCP": "n1-highmem-4"},
                    "enable_elastic_disk": "true",
                    "docker_image": {
                        "url": "projectglow/databricks-glow:1.2.1",
                        "basic_auth": {
                            "username": docker_username,
                            "password": docker_password
                        }
                    },
                    "data_security_mode": "NONE",
                    "num_workers": 2
                }
            },
            {
                "job_cluster_key": "single_node",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "10.4.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_D3_v2", "GCP": "n1-highmem-4"},
                    "enable_elastic_disk": "true",
                    "docker_image": {
                        "url": "projectglow/databricks-glow:1.2.1",
                        "basic_auth": {
                            "username": docker_username,
                            "password": docker_password
                        }
                    },
                    "data_security_mode": "NONE",
                    "num_workers": 0
                }
            },
            {
                "job_cluster_key": "hail",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "9.1.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_D3_v2", "GCP": "n1-highmem-4"},
                    "enable_elastic_disk": "true",
                    "docker_image": {
                        "url": "projectglow/databricks-hail:0.2.85",
                        "basic_auth": {
                            "username": docker_username,
                            "password": docker_password
                        }
                    },
                    "data_security_mode": "NONE",
                    "num_workers": 2
                }
            },
            {
                "job_cluster_key": "hail_to_glow",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "9.1.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_D3_v2", "GCP": "n1-highmem-4"},
                    "enable_elastic_disk": "true",
                    "docker_image": {
                        "url": "projectglow/databricks-hail:0.2.85",
                        "basic_auth": {
                            "username": docker_username,
                            "password": docker_password
                        }
                    },
                    "data_security_mode": "NONE",
                    "num_workers": 2
                }
            }
        ]
}
  

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)

# COMMAND ----------



# COMMAND ----------


