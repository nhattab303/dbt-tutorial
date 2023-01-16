{% set source_name='ods_job_post_status_updates_stg' %}
{% set source_table= 'ods_indeed_' + var("site")|string|lower + '_status_stg' %}
{% set table_alias= 'dwh_indeed_' + var("site")|string|lower + '_status_updates' %}

{{ config(materialized='incremental', alias=table_alias, unique_key='job_id', schema='dwh') }}

/*
# DWH Indeed Status Updates Loader

This model loads job posts status metadata from ODS to DWH.
Expects site variable.
site variable can be one of ['IL', 'UAE', 'US']
*/

with indeed_status_updates as (
    select
        *,
        current_timestamp() as dwh_updated_at
    from
        {{ ref(source_table) }}
)
select
    *
from
    indeed_status_updates
{% if is_incremental() %}
    where
        indeed_status_updates.load_date>(select ifnull(max(d.stg_updated_at), date('1970-01-01'))
                    from {{this}} d )
{% endif %}
