

{{ config(materialized='incremental', schema='EXTRACTOR_SERVICES', unique_key='job_id')}}


with mergeddesods as(
    select
        job_id as job_id,
        status as status,
        title as title,
        description as description,
        open_date as open_date,
        close_date as close_date,
        des_created_at as des_created_at,
        ods_created_at as ods_created_at

    from {{ ref('desjpposts') }}
    join {{ ref('odsalljobs') }}
    using (job_id)

    {% if is_incremental() %}

      where des_created_at > (select max(g.updated_at) from {{ this }} as g)
      or ods_created_at > (select max(g.updated_at) from {{ this }} as g)

    {% endif %}
)

select
    job_id as job_id,
    status as status,
    title as title,
    description as description,
    open_date as open_date,
    close_date as close_date,
    current_timestamp() as updated_at
from mergeddesods
--where job_id = 514215742815118


