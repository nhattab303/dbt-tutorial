
-- Use the `ref` function to select from other models

with ods_data_source as (
    select
        id as job_id,
        json_data:title as title,
        json_data:description as description,
        json_data:date_posted as open_date
    from {{ref('DEV_DB.ODS.ODS_ALLJOBS_ENRICHED')}}
)

select *
from ods_data_source