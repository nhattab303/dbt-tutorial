{% set source_table_var= 'ods_indeed_il_jps_status' %}

{{ config(materialized='incremental', unique_key='job_id', schema='STG',
merge_update_columns=['job_id', 'close_date', 'status', 'last_checked', 'load_date', 'stg_updated_at']) }}

{{ transform_jps_ods_to_stg(source_table_var) }}