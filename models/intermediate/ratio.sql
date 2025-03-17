--SELECT
--    ROUND(SUM(CASE WHEN last_cause = "email-click" THEN 1 ELSE 0 END)/count(*),3) as response_ratio,
--    SUM(CASE WHEN last_cause = "email-click" AND interested_date IS NOT NULL THEN 1 ELSE 0 END)/SUM(CASE WHEN last_cause = "email-click" THEN 1 ELSE 0 END) as interested_reponse_active_ratio,
--    SUM(CASE WHEN last_cause = "email-click" AND not_interested_date IS NOT NULL THEN 1 ELSE 0 END)/SUM(CASE WHEN last_cause = "email-click" THEN 1 ELSE 0 END) as not_interested_reponse_active_ratio
--FROM {{ ref('3_tables_joined') }}
SELECT * FROM (
    SELECT 
    round(nb_optin/nb_distinct_candidates,2) as optin_ratio,
    round(nb_optin_qualified/nb_optin,2) as optin_to_optin_qualified,
    round(nb_shortlisted_candidate/nb_optin_qualified,2) as qualified_to_shortlisted,
    round(nb_answers/nb_shortlisted_candidate,2) as shortlist_answers_rate,
    round(nb_interested_total/nb_answers,2) as interested_rate,
    round(1-(nb_interested_total/nb_answers),2) as not_interested_rate,
    round(nb_company_answer/nb_interested_total,2) as company_answers_rate_from_interested,
    round(nb_approved/nb_company_answer,2) as approved_from_company_answers,
    round(1-(nb_approved/nb_company_answer),2) as declined_from_company_answers

FROM {{ ref('join_optin_shortlist') }}
)
PIVOT 
(
    MAX(optin_ratio)

)