SELECT
  AVG(TIMESTAMP_DIFF(last_receive_time, awaiting_date, HOUR)) AS avg_timeout_awating,
  COUNT(DISTINCT user_id) AS timeout_count
FROM {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }}
--WHERE last_cause = 'auto-timeout';