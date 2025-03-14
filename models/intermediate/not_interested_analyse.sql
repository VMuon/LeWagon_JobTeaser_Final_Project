SELECT
  last_status_update,
  last_cause,
  COUNT(DISTINCT user_id) AS user_count,
  ROUND(COUNT(DISTINCT user_id) / SUM(COUNT(DISTINCT user_id)) OVER () * 100, 2) AS percentage
FROM {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }}
WHERE last_status_update = 'not interested' OR last_cause = 'auto-timeout'
GROUP BY last_status_update, last_cause,last_cause
ORDER BY user_count DESC