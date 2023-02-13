{% set source_table_var= 'stg_indeed_il_jps_status' %}

{{ config(materialized='incremental', unique_key='job_id', schema='DWH',
 merge_update_columns=['job_id', 'created_date', 'close_date', 'status', 'last_checked', 'load_date', 'dwh_updated_at']) }}

{{ load_jps_status_to_dwh(source_table_var) }}