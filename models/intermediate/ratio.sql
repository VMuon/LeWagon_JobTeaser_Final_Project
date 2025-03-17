SELECT
    ROUND(SUM(CASE WHEN last_cause = "email-click" THEN 1 ELSE 0 END)/count(*),3) as response_ratio,
    SUM(CASE WHEN last_cause = "email-click" AND interested_date IS NOT NULL THEN 1 ELSE 0 END)/SUM(CASE WHEN last_cause = "email-click" THEN 1 ELSE 0 END) as interested_reponse_active_ratio,
    SUM(CASE WHEN last_cause = "email-click" AND not_interested_date IS NOT NULL THEN 1 ELSE 0 END)/SUM(CASE WHEN last_cause = "email-click" THEN 1 ELSE 0 END) as not_interested_reponse_active_ratio
FROM {{ ref('3_tables_joined') }}