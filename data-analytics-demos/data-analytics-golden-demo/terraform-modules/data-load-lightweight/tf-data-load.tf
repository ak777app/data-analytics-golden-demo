####################################################################################
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
####################################################################################

####################################################################################
# Loads the demo data without Cloud Composer (only used when deploy_lightweight = true)
####################################################################################

variable "project_id" {}
variable "bigquery_region" {}
variable "raw_bucket_name" {}
variable "processed_bucket_name" {}
variable "code_bucket_name" {}
variable "rideshare_raw_bucket_name" {}
variable "bigquery_rideshare_llm_curated_dataset" {}
variable "curl_impersonation" {}
variable "taxi_data_years" {
  type = list(string)
}

resource "null_resource" "lightweight_data_load" {
  triggers = {
    script_sha256   = filesha256("${path.module}/lightweight-data-load.sh")
    taxi_data_years = join(",", var.taxi_data_years)
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = "bash ${path.module}/lightweight-data-load.sh"
    environment = {
      PROJECT_ID                    = var.project_id
      BIGQUERY_REGION               = var.bigquery_region
      RAW_BUCKET                    = var.raw_bucket_name
      PROCESSED_BUCKET              = var.processed_bucket_name
      CODE_BUCKET                   = var.code_bucket_name
      RIDESHARE_RAW_BUCKET          = var.rideshare_raw_bucket_name
      RIDESHARE_LLM_CURATED_DATASET = var.bigquery_rideshare_llm_curated_dataset
      TAXI_DATA_YEARS               = join(" ", var.taxi_data_years)
      IMPERSONATION                 = var.curl_impersonation
    }
  }
}
