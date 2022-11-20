


select *
from {{ ref('desjpposts') }}
join {{ ref('odsalljobs') }}
using (job_id)
