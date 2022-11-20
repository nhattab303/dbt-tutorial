
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

--{{ config(materialized='table') }}
{{ config(materialized='table', schema='EXTRACTOR_SERVICES')}}

with des_source_data as (
    select
        to_varchar(id) as job_id,
        status as status,
        date(close_date) as close_date
    from {{source('des_job_posts','ALLJOBS_JOB_POSTS')}}
)

select *
from des_source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
