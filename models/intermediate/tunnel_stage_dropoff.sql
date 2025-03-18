###compare optin user/ dropoff
SELECT
  stage,
  optin_user_count,
  stage_order,
  LAG( optin_user_count) OVER (ORDER BY stage_order) AS previous_count,
  CASE
    WHEN LAG( optin_user_count) OVER (ORDER BY stage_order) IS NOT NULL
    THEN ROUND((LAG( optin_user_count) OVER (ORDER BY stage_order) -  optin_user_count) / LAG( optin_user_count) OVER (ORDER BY stage_order) * 100, 2)
    ELSE NULL
  END AS dropoff_percentage
FROM {{ ref('funnel_stage_agregated') }}
ORDER BY stage_order;