

--{{ config(materialized='table') }}

{{ config(materialized='incremental', schema='EXTRACTOR_SERVICES', unique_key='job_id')}}


with des_source_data as (
    select
        to_varchar(id) as job_id,
        status as status,
        date(close_date) as close_date,
        time_created as loaded_at
    from {{source('des_job_posts','ALLJOBS_JOB_POSTS')}}
    {% if is_incremental() %}
      where loaded_at > (select max(g.des_created_at) from {{ this }} as g)
    {% endif %}
)

select
    job_id as job_id,
    status as status,
    close_date as close_date,
    current_timestamp() as des_created_at
from des_source_data
--where job_id = 154815181211215