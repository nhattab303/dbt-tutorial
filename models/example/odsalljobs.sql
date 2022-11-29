
-- Use the `ref` function to select from other models


{{ config(materialized='incremental', schema='ODS', unique_key='job_id')}}

with ods_data_source as (
    select
        id as job_id,
        json_data:title as title,
        json_data:description as description,
        date(json_data:date_posted) as open_date,
        load_date as loaded_at
    from {{source('ods_job_posts','ODS_ALLJOBS_ENRICHED')}} as src
    where
        nvl(src.json_data:title, '') != ''
        {% if is_incremental() %}
            and loaded_at > (select max(g.ods_created_at) from {{ this }} as g)
        {% endif %}
)

select
    job_id as job_id,
    title as title,
    description as description,
    open_date as open_date,
    current_timestamp() as ods_created_at
from ods_data_source
--where job_id = 6913841231815