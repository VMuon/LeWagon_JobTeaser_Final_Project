SELECT 1 AS ordre, "optin_ratio" AS metric, round(nb_optin/nb_distinct_candidates,2) AS value FROM {{ ref('join_optin_shortlist') }}
UNION ALL
SELECT 2, "optin_to_optin_qualified", round(nb_optin_qualified/nb_optin,2) FROM {{ ref('join_optin_shortlist') }}
UNION ALL
SELECT 3, "qualified_to_shortlisted", round(nb_shortlisted_candidate/nb_optin_qualified,2) FROM {{ ref('join_optin_shortlist') }}
UNION ALL
SELECT 4, "shortlist_answers_rate", round(nb_answers/nb_shortlisted_candidate,2) FROM {{ ref('join_optin_shortlist') }}
UNION ALL
SELECT 5, "interested_rate", round(nb_interested_total/nb_answers,2) FROM {{ ref('join_optin_shortlist') }}
UNION ALL
SELECT 6, "not_interested_rate", round(1-(nb_interested_total/nb_answers),2) FROM {{ ref('join_optin_shortlist') }}
UNION ALL
SELECT 7, "company_answers_rate_from_interested", round(nb_company_answer/nb_interested_total,2) FROM {{ ref('join_optin_shortlist') }}
UNION ALL
SELECT 8, "approved_from_company_answers", round(nb_approved/nb_company_answer,2) FROM {{ ref('join_optin_shortlist') }}
UNION ALL
SELECT 9, "declined_from_company_answers", round(1-(nb_approved/nb_company_answer),2) FROM {{ ref('join_optin_shortlist') }}
ORDER BY CAST(ordre AS INT64)