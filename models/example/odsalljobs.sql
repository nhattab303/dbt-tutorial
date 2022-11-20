
-- Use the `ref` function to select from other models

{{ config(materialized='table', schema='ODS')}}

with ods_data_source as (
    select
        id as job_id,
        json_data:title as title,
        json_data:description as description,
        date(json_data:date_posted) as open_date
    from {{source('ods_job_posts','ODS_ALLJOBS_ENRICHED')}} as src
    where
        nvl(src.json_data:title, '') != ''
)

select *
from ods_data_source