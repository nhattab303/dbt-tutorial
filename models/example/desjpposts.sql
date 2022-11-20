
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with des_source_data as (
    select
        id as job_id,
        status as status,
        date(date_closed) as close_date
    from {{ref('DEV_DB.EXTRACTOR_SERVICES.ALLJOBS_JOB_POSTS')}}
)

select *
from des_source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
