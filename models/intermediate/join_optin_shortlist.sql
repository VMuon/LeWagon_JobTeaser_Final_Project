with
    optin_shortlist as (
        select
            optin.user_id,
            shortlist.shortlist_id,
            optin.last_opt_status,
            optin.last_resume_uploaded
        from {{ ref("stg_jobteaser_lewagon__optin_aggregated") }} as optin
        left join
            {{ ref("stg_jobteaser_lewagon__candidate_status_aggregated") }} as shortlist
            using (user_id)
    )

select
    round(sum(case when last_opt_status = true and last_resume_uploaded = true then 1 else 0 end) / sum(case when last_opt_status = true then 1 else 0 end),2) as optin_qualified_ratio,
    round(count(shortlist_id) / count(*), 2) as optin_to_shortlist_ratio,
    round(count(shortlist_id) / sum(case when last_opt_status = true and last_resume_uploaded = true then 1 else 0 end),2) as optin_qualified_to_shortlist_ratio

from optin_shortlist
