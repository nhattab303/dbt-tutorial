
{{ config(materialized='incremental', schema='EXTRACTOR_SERVICES')}}


select *
from {{ ref('desjpposts') }}
join {{ ref('odsalljobs') }}
using (job_id)
