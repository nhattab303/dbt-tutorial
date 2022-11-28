
{{ config(materialized='incremental', schema='EXTRACTOR_SERVICES')}}

# if incremental

select *
from {{ ref('desjpposts') }}
join {{ ref('odsalljobs') }}
using (job_id)
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_time > (select max(event_time) from {{ this }})

{% endif %}


