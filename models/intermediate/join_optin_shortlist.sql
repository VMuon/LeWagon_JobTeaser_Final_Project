with
    optin_shortlist as (
        select
            optin.user_id,
            shortlist.shortlist_id,
            optin.last_opt_status,
            optin.last_resume_uploaded,
            shortlist.last_status_update,
            shortlist.interested_date,
            shortlist.last_cause
        from {{ ref("stg_jobteaser_lewagon__optin_aggregated") }} as optin
        left join
            {{ ref("stg_jobteaser_lewagon__candidate_status_aggregated") }} as shortlist
            using (user_id)
    )

select
    count(distinct user_id) as nb_distinct_candidates,
    sum(case when last_opt_status = true then 1 else 0 end) as nb_optin,
    sum(case when last_opt_status = true and last_resume_uploaded = true then 1 else 0 end) as nb_optin_qualified,
    count(shortlist_id) as nb_shortlisted_candidate,
    sum(case when last_cause ="email-click" then 1 else 0 end) as nb_answers,
    sum(case when interested_date is not null then 1 else 0 end) as nb_interested_total,
    sum(case when last_status_update = "interested" then 1 else 0 end) as nb_interested,
    sum(case when last_status_update = "approved" or last_status_update = "declined" then 1 else 0 end) as nb_company_answer,   
    sum(case when last_status_update = "approved" then 1 else 0 end) as nb_approved,
    sum(case when last_status_update = "declined" then 1 else 0 end) as nb_declined
    --round(sum(case when last_opt_status = true and last_resume_uploaded = true then 1 else 0 end) / sum(case when last_opt_status = true then 1 else 0 end),2) as optin_qualified_ratio,
    --round(count(shortlist_id) / count(*), 2) as optin_to_shortlist_ratio,
    --round(count(shortlist_id) / sum(case when last_opt_status = true and last_resume_uploaded = true then 1 else 0 end),2) as optin_qualified_to_shortlist_ratio,
    --round(sum(case when last_status_update = "approved" then 1 else 0 end)/sum(case when interested_date is not null then 1 else 0 end),2) as interested_to_approved_ratio,
    --round(sum(case when last_status_update = "declined" then 1 else 0 end)/sum(case when interested_date is not null then 1 else 0 end),2) as interested_to_declined_ratio

from optin_shortlist

