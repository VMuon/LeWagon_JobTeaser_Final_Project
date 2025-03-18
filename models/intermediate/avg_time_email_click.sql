#####time email click

WITH email_click_responses AS (
  SELECT
    user_id,
    shortlist_id,
    awaiting_date,
    interested_date,
    not_interested_date,
    last_status_update,
    last_cause,
    CASE
      WHEN last_status_update = 'interested' AND last_cause = 'email-click' AND interested_date IS NOT NULL
        THEN TIMESTAMP_DIFF(interested_date, awaiting_date, HOUR)
      WHEN last_status_update = 'not interested' AND last_cause = 'email-click' AND not_interested_date IS NOT NULL
        THEN TIMESTAMP_DIFF(not_interested_date, awaiting_date, HOUR)
      ELSE NULL
    END AS response_time_hours
  FROM {{ source('jobteaser_lewagon', 'candidate_status_aggregated') }}
  WHERE last_cause = 'email-click'
    AND awaiting_date IS NOT NULL
    AND (interested_date IS NOT NULL OR not_interested_date IS NOT NULL)
    AND (
      (last_status_update = 'interested' AND interested_date >= awaiting_date) OR
      (last_status_update = 'not interested' AND not_interested_date >= awaiting_date)
    )
)
SELECT
  AVG(CASE WHEN last_status_update = 'interested' THEN response_time_hours END) AS avg_interested_response_hours,
  ----COUNT(CASE WHEN last_status_update = 'interested' THEN 1 END) AS interested_count,
  AVG(CASE WHEN last_status_update = 'not interested' THEN response_time_hours END) AS avg_not_interested_response_hours,
  ---COUNT(CASE WHEN last_status_update = 'not interested' THEN 1 END) AS not_interested_count,
  AVG(response_time_hours) AS time_email_click_hours,
  ---COUNT(*) AS avg_email_click_responses
FROM email_click_responses
WHERE response_time_hours IS NOT NULL;
