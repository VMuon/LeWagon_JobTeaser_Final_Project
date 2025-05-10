WITH optin_shortlist AS (
    SELECT
        optin.user_id,
        shortlist.shortlist_id,
        optin.last_opt_status,
        optin.last_resume_uploaded,
        shortlist.last_status_update,
        shortlist.interested_date,
        shortlist.last_cause
    FROM {{ ref("stg_jobteaser_lewagon__optin_aggregated") }} AS optin
    LEFT JOIN
        {{ ref("stg_jobteaser_lewagon__candidate_status_aggregated") }} AS shortlist
        USING (user_id)
),
aggregated_data AS (
    SELECT
        count(distinct user_id) AS nb_distinct_candidates,
        sum(CASE WHEN last_opt_status = true THEN 1 ELSE 0 END) AS nb_optin,
        sum(CASE WHEN last_opt_status = true AND last_resume_uploaded = true THEN 1 ELSE 0 END) AS nb_optin_qualified,
        count(shortlist_id) AS nb_shortlisted_candidate,
        sum(CASE WHEN last_cause = "email-click" THEN 1 ELSE 0 END) AS nb_answers,
        sum(CASE WHEN interested_date IS NOT NULL THEN 1 ELSE 0 END) AS nb_interested_total,
        --sum(CASE WHEN last_status_update = "interested" THEN 1 ELSE 0 END) AS nb_interested,
        sum(CASE WHEN last_status_update = "approved" OR last_status_update = "declined" THEN 1 ELSE 0 END) AS nb_company_answer,   
        sum(CASE WHEN last_status_update = "approved" THEN 1 ELSE 0 END) AS nb_approved,
        --sum(CASE WHEN last_status_update = "declined" THEN 1 ELSE 0 END) AS nb_declined
    FROM optin_shortlist
)

SELECT 
    metric,
    value,
    CASE metric
        WHEN "nb_distinct_candidates" THEN 1
        WHEN "nb_optin" THEN 2
        WHEN "nb_optin_qualified" THEN 3
        WHEN "nb_shortlisted_candidate" THEN 4
        WHEN "nb_answers" THEN 5
        WHEN "nb_interested_total" THEN 6
        --WHEN "nb_interested" THEN 7
        WHEN "nb_company_answer" THEN 7
        --WHEN "nb_declined" THEN 9
        WHEN "nb_approved" THEN 8
    END AS ordre
FROM aggregated_data
UNPIVOT (
    value FOR metric IN (
        nb_distinct_candidates,
        nb_optin,
        nb_optin_qualified,
        nb_shortlisted_candidate,
        nb_answers,
        nb_interested_total,
        --nb_interested,
        nb_company_answer,
        --nb_declined,
        nb_approved
    )
) AS unpvt
ORDER BY ordre
