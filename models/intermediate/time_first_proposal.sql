####first time proposal

WITH OptinData AS (
  SELECT user_id, optin_date
  FROM {{ ref('stg_jobteaser_lewagon__optin_aggregated') }} 
  WHERE optin_date IS NOT NULL
),
FirstShortlist AS (
  SELECT c.user_id, MIN(c.last_receive_time) AS first_shortlist_time
  FROM {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }} c
  JOIN OptinData o
    ON c.user_id = o.user_id AND c.last_receive_time >= o.optin_date
  WHERE c.last_status_update = 'awaiting'
  GROUP BY c.user_id
)
SELECT AVG(TIMESTAMP_DIFF(first_shortlist_time, optin_date, HOUR)) AS avg_time_to_first_proposal_hours
FROM OptinData o
JOIN FirstShortlist f
  ON o.user_id = f.user_id;