with agg as (

select 
    shortlist_id,
    max(case when approved_date is not null then "shortlist has an approved" else "shortlist has no approved" end) as shortlist_approved
    --max(case when last_status_update = "approved" then "shortlist has an approved" else "shortlist has no approved" end) as shortlist_approved
from {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }}
group by shortlist_id

)

select 
    shortlist_approved, 
    count(*)
from agg
group by shortlist_approved