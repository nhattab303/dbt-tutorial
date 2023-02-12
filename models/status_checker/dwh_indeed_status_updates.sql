{% set source_name_var='stg_jps_status' %}
{% set source_table_var= 'stg_indeed_' + var("site")|string|lower + '_jps_status' %}
{% set table_alias_var= 'dwh_indeed_' + var("site")|string|lower + '_jps_status' %}

{{ config(materialized='incremental', alias=table_alias_var, unique_key='job_id',
 merge_update_columns=['job_id', 'created_date', 'close_date', 'status', 'last_checked', 'load_date', 'dwh_updated_at']) }}

/*
# DWH Indeed Status Updates Loader

This model loads job posts status metadata from ODS to DWH.
Expects site variable.
site variable can be one of ['IL', 'UAE', 'US']
*/

select
    job_id,
    created_date,
    close_date,
    status,
    last_checked,
    load_date,
    current_timestamp() as dwh_created_at,
    current_timestamp() as dwh_updated_at

from
    {{ source(source_name_var, source_table_var) }} isu
{% if is_incremental() %}
    where
        isu.stg_updated_at>(select ifnull(max(dwh_updated_at), date('1970-01-01'))
                    from {{this}})
{% endif %}
