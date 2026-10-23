#!/bin/bash

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
# Loads the demo data without Cloud Composer (deploy_lightweight = true).
# Replaces these Airflow DAGs: step-01/02 quick-copy, step-03-hydrate-tables,
# sample-seed-unstructured-data, sample-rideshare-download-images,
# sample-rideshare-hydrate-data and sample-rideshare-llm-hydrate-data.
# Only the taxi years in TAXI_DATA_YEARS are copied to keep storage/query costs low.
# Core taxi/thelook steps fail the deployment on error; optional demo steps only warn.
####################################################################################

set -euo pipefail

: "${PROJECT_ID:?}" "${BIGQUERY_REGION:?}" "${RAW_BUCKET:?}" "${PROCESSED_BUCKET:?}" "${CODE_BUCKET:?}"
: "${RIDESHARE_RAW_BUCKET:?}" "${RIDESHARE_LLM_CURATED_DATASET:?}" "${TAXI_DATA_YEARS:?}"
IMPERSONATION="${IMPERSONATION:-}"

SOURCE_BUCKET="gs://data-analytics-golden-demo"
WARNINGS=()

gcs() {
  # shellcheck disable=SC2086
  gcloud storage "$@" ${IMPERSONATION}
}

access_token() {
  # shellcheck disable=SC2086
  gcloud auth print-access-token ${IMPERSONATION}
}

json_field() {
  python3 -c "$1"
}

# Runs a BigQuery SQL statement through the REST API and waits for it to finish
run_sql() {
  local sql="$1" body response job_id state
  echo "Running SQL: ${sql}"
  body=$(python3 -c 'import json,sys; print(json.dumps({"configuration":{"query":{"query":sys.argv[1],"useLegacySql":False}},"jobReference":{"location":sys.argv[2]}}))' "${sql}" "${BIGQUERY_REGION}")
  response=$(curl -sS -X POST \
    -H "Authorization: Bearer $(access_token)" \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_ID}/jobs")
  job_id=$(echo "${response}" | json_field 'import json,sys; print(json.load(sys.stdin).get("jobReference",{}).get("jobId",""))')
  if [ -z "${job_id}" ]; then
    echo "Failed to submit BigQuery job: ${response}"
    return 1
  fi

  while true; do
    sleep 10
    response=$(curl -sS \
      -H "Authorization: Bearer $(access_token)" \
      "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_ID}/jobs/${job_id}?location=${BIGQUERY_REGION}")
    state=$(echo "${response}" | json_field 'import json,sys
d=json.load(sys.stdin); s=d.get("status",{})
if "error" in d: print("ERROR:"+json.dumps(d["error"]))
elif "errorResult" in s: print("ERROR:"+json.dumps(s["errorResult"]))
else: print(s.get("state","UNKNOWN"))')
    case "${state}" in
      DONE) echo "BigQuery job ${job_id} done"; return 0 ;;
      ERROR:*) echo "BigQuery job ${job_id} failed: ${state#ERROR:}"; return 1 ;;
    esac
  done
}

# Optional steps: log a warning instead of failing the deployment
try_step() {
  local description="$1"; shift
  if ! "$@"; then
    echo "WARNING: optional step failed: ${description}"
    WARNINGS+=("${description}")
    return 1
  fi
}

####################################################################################
# Taxi data (step-01 / step-02 quick copy), limited to TAXI_DATA_YEARS
# Parquet is copied for every selected year, CSV/JSON only for January (format demos)
####################################################################################
copy_taxi_data() {
  local year color fmt table
  for year in ${TAXI_DATA_YEARS}; do
    for color in yellow green; do
      gcs cp --recursive "${SOURCE_BUCKET}/raw-bucket-v2/raw/taxi-data/${color}/${year}" "gs://${RAW_BUCKET}/raw/taxi-data/${color}/"
      gcs cp --recursive "${SOURCE_BUCKET}/processed-bucket-v2/processed/taxi-data/${color}/trips_table/parquet/year=${year}" \
        "gs://${PROCESSED_BUCKET}/processed/taxi-data/${color}/trips_table/parquet/"
      for fmt in csv json; do
        gcs cp --recursive "${SOURCE_BUCKET}/processed-bucket-v2/processed/taxi-data/${color}/trips_table/${fmt}/year=${year}/month=1" \
          "gs://${PROCESSED_BUCKET}/processed/taxi-data/${color}/trips_table/${fmt}/year=${year}/"
      done
    done
  done

  for table in vendor_table rate_code_table payment_type_table trip_type_table location_table; do
    gcs cp --recursive "${SOURCE_BUCKET}/processed-bucket-v2/processed/taxi-data/${table}" "gs://${PROCESSED_BUCKET}/processed/taxi-data/"
  done
}

copy_delta_io_data() {
  gcs cp --recursive "${SOURCE_BUCKET}/processed-bucket/delta_io" "gs://${PROCESSED_BUCKET}/"
}

####################################################################################
# Unstructured data, TF models and rideshare images
####################################################################################
seed_unstructured_data() {
  local work_dir file
  work_dir=$(mktemp -d) || return 1
  gcs cp --recursive gs://cloud-samples-data/vision "gs://${RAW_BUCKET}/biglake-unstructured-data/" || return 1
  for file in face/face_no_surprise.jpg pdf_tiff/census2010.pdf celebrity_recognition.jpg document_parsing/key_value_pairs.pdf text/screen.jpg; do
    gcs objects update --custom-metadata="pii=true" "gs://${RAW_BUCKET}/biglake-unstructured-data/vision/${file}" || return 1
  done

  mkdir -p "${work_dir}/resnet_50_classification_1" "${work_dir}/imagenet_mobilenet_v3_small_075_224_feature_vector_5" || return 1
  curl -sSfL "https://tfhub.dev/tensorflow/resnet_50/classification/1?tf-hub-format=compressed" | tar xz -C "${work_dir}/resnet_50_classification_1" || return 1
  curl -sSfL "https://tfhub.dev/google/imagenet/mobilenet_v3_small_075_224/feature_vector/5?tf-hub-format=compressed" | tar xz -C "${work_dir}/imagenet_mobilenet_v3_small_075_224_feature_vector_5" || return 1
  gcs cp --recursive "${work_dir}/resnet_50_classification_1" "gs://${CODE_BUCKET}/tf-models/" || return 1
  gcs cp --recursive "${work_dir}/imagenet_mobilenet_v3_small_075_224_feature_vector_5" "gs://${CODE_BUCKET}/tf-models/" || return 1
  rm -rf "${work_dir}" || return 1
}

copy_rideshare_images() {
  local work_dir
  work_dir=$(mktemp -d) || return 1
  curl -sSfL "https://images.pexels.com/photos/1008155/pexels-photo-1008155.jpeg" --output "${work_dir}/pexels-photo-1008155.jpeg" || return 1
  gcs cp "${work_dir}/pexels-photo-1008155.jpeg" "gs://${RIDESHARE_RAW_BUCKET}/rideshare_images/pexels-photo-1008155.jpeg" --custom-metadata="location_id=1" || return 1
  gcs cp "${SOURCE_BUCKET}/rideshare-lakehouse-raw-bucket/rideshare_images/*" "gs://${RIDESHARE_RAW_BUCKET}/rideshare_images/" || return 1
  rm -rf "${work_dir}" || return 1
}

####################################################################################
# Rideshare LLM (sample-rideshare-llm-hydrate-data)
####################################################################################
hydrate_rideshare_llm() {
  gcs cp --recursive "${SOURCE_BUCKET}/rideshare-lakehouse-raw-bucket/rideshare_llm_export/v1/*" "gs://${RIDESHARE_RAW_BUCKET}/rideshare_llm_export/" || return 1
  gcs cp --recursive "${SOURCE_BUCKET}/rideshare-lakehouse-raw-bucket/rideshare_audios/v1/*.mp3" "gs://${RIDESHARE_RAW_BUCKET}/rideshare_audios/" || return 1
  run_sql "CALL \`${PROJECT_ID}.${RIDESHARE_LLM_CURATED_DATASET}.sp_reset_demo\`();" || return 1
}

####################################################################################
# Rideshare Lakehouse (sample-rideshare-hydrate-data)
# The Iceberg step runs on BigQuery serverless Spark (spark-connection) instead of
# a Composer-triggered Dataproc batch. The *_streaming_view procedures are skipped
# because they read the Dataflow streaming tables, which are not deployed.
####################################################################################
hydrate_rideshare_lakehouse() {
  local raw="${PROJECT_ID}.rideshare_lakehouse_raw"
  local enriched="${PROJECT_ID}.rideshare_lakehouse_enriched"
  local curated="${PROJECT_ID}.rideshare_lakehouse_curated"

  run_sql "CALL \`${raw}.sp_create_raw_data\`();" || return 1
  try_step "rideshare_lakehouse_raw.sp_create_biglake_object_table" run_sql "CALL \`${raw}.sp_create_biglake_object_table\`();" || true
  run_sql "CALL \`${raw}.sproc_sp_create_biglake_tables\`();" || return 1
  run_sql "CALL \`${enriched}.sp_process_data\`();" || return 1
  if ! try_step "rideshare_lakehouse_enriched.sp_iceberg_spark_transformation (BigQuery serverless Spark)" \
      run_sql "CALL \`${enriched}.sp_iceberg_spark_transformation\`();"; then
    echo "Skipping the rideshare enriched/curated steps because they read the Iceberg tables created by Spark"
    return 1
  fi
  try_step "rideshare_lakehouse_enriched.sp_unstructured_data_analysis" run_sql "CALL \`${enriched}.sp_unstructured_data_analysis\`();" || true
  run_sql "CALL \`${curated}.sp_process_data\`();" || return 1
  try_step "rideshare_lakehouse_curated.sp_create_looker_studio_view" run_sql "CALL \`${curated}.sp_create_looker_studio_view\`();" || true
  try_step "rideshare_lakehouse_curated.sp_create_website_realtime_dashboard" run_sql "CALL \`${curated}.sp_create_website_realtime_dashboard\`();" || true
  try_step "rideshare_lakehouse_curated.sp_model_training" run_sql "CALL \`${curated}.sp_model_training\`();" || true
}

####################################################################################
# Main
####################################################################################
echo "Lightweight data load: taxi years [${TAXI_DATA_YEARS}]"

# Core (fail the deployment on error)
copy_taxi_data
run_sql "CALL \`${PROJECT_ID}.taxi_dataset.sp_create_taxi_external_tables\`();"
run_sql "CALL \`${PROJECT_ID}.taxi_dataset.sp_create_taxi_internal_tables\`();"
run_sql "CALL \`${PROJECT_ID}.taxi_dataset.sp_create_taxi_biglake_tables\`();"
run_sql "CALL \`${PROJECT_ID}.thelook_ecommerce.create_product_deliveries\`();"
run_sql "CALL \`${PROJECT_ID}.thelook_ecommerce.create_thelook_tables\`('${BIGQUERY_REGION}');"

# Optional demo data (warn on error)
try_step "copy delta_io sample data" copy_delta_io_data || true
try_step "seed unstructured data / TF models" seed_unstructured_data || true
try_step "copy rideshare images" copy_rideshare_images || true
try_step "rideshare lakehouse hydration" hydrate_rideshare_lakehouse || true
try_step "rideshare LLM hydration" hydrate_rideshare_llm || true

if [ ${#WARNINGS[@]} -gt 0 ]; then
  echo "Lightweight data load finished with warnings (optional steps that failed):"
  printf '  - %s\n' "${WARNINGS[@]}"
  echo "Re-run with: terraform apply -replace='module.data-load-lightweight-module[0].null_resource.lightweight_data_load' ..."
else
  echo "Lightweight data load finished"
fi
