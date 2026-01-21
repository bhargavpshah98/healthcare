resource "aws_glue_catalog_database" "healthcare_db" {
  name = "${var.project_name}_db"
}

resource "aws_glue_catalog_table" "medicare_claims" {
  name          = "medicare_claims"
  database_name = aws_glue_catalog_database.healthcare_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
    "compressionType" = "none"
    "typeOfData"      = "file"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.processed.bucket}/output/medicare_claims/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "rndrng_npi"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_last_org_name"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_first_name"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_mi"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_crdntls"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_ent_cd"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_st1"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_st2"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_city"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_state_abrvtn"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_state_fips"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_zip5"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_ruca"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_ruca_desc"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_cntry"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_type"
      type = "string"
    }
    columns {
      name = "rndrng_prvdr_mdcr_prtcptg_ind"
      type = "string"
    }
    columns {
      name = "hcpcs_cd"
      type = "string"
    }
    columns {
      name = "hcpcs_desc"
      type = "string"
    }
    columns {
      name = "hcpcs_drug_ind"
      type = "string"
    }
    columns {
      name = "place_of_srvc"
      type = "string"
    }
    columns {
      name = "tot_benes"
      type = "bigint"
    }
    columns {
      name = "tot_srvcs"
      type = "double"
    }
    columns {
      name = "tot_bene_day_srvcs"
      type = "double"
    }
    columns {
      name = "total_submitted_charge_amount"
      type = "double"
    }
    columns {
      name = "total_medicare_payment_amount"
      type = "double"
    }
    columns {
      name = "avg_mdcr_alowd_amt"
      type = "double"
    }
    columns {
      name = "avg_mdcr_stdzd_amt"
      type = "double"
    }
  }
}

resource "aws_glue_job" "healthcare_etl" {
  name     = "${var.project_name}-etl-job"
  role_arn = aws_iam_role.glue_role.arn

  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.bucket}/scripts/transform.py"
    python_version  = "3"
  }

  # Keep costs low for assignment
  worker_type       = "G.1X"
  number_of_workers = 2

  default_arguments = {
    "--job-language"                     = "python"
    "--RAW_BUCKET"                       = aws_s3_bucket.raw.bucket
    "--PROCESSED_BUCKET"                 = aws_s3_bucket.processed.bucket
    "--enable-metrics"                   = ""
    "--enable-continuous-cloudwatch-log" = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}
