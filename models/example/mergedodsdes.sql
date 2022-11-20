

select *
from {{ ref('desjpposts') }}
join {{ ref('odsalljobs') }}

