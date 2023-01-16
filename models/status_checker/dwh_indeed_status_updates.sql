{% set source_name='ods_job_post_status_updates_stg' %}
{% set source_table= 'ods_indeed_' + var("site")|string|lower + '_status_stg' %}
{% set table_alias= 'dwh_indeed_' + var("site")|string|lower + '_status_updates' %}

{{ config(materialized='incremental', alias=table_alias, unique_key='job_id') }}

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
        {{ source(source_name, source_table) }}
)
select
    job_id as job_id,
    creation_date as creation_date,
    close_date as close_date,
    IFF(is_closed, 'closed', 'open') as status,
    last_checked as last_checked,
    current_timestamp() as dwh_updated_at,
    load_date as load_date
from
    indeed_status_updates
{% if is_incremental() %}
    where
        indeed_status_updates.stg_created_at>(select ifnull(max(d.load_date), date('1970-01-01'))
                    from {{this}} d )
{% endif %}