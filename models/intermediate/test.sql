-- Create a unified table for funnel stages

WITH Optin AS (
  SELECT
    user_id,
    MAX(CASE WHEN last_opt_status = TRUE THEN 1 ELSE 0 END) AS is_optin,
    MAX(CASE WHEN last_opt_status = TRUE AND last_resume_uploaded = TRUE THEN 1 ELSE 0 END) AS is_optin_qualified
  FROM {{ ref('stg_jobteaser_lewagon__optin_aggregated') }}
  GROUP BY user_id
),
Candidate AS (
  SELECT
    user_id,
    MAX(CASE WHEN last_status_update = 'Shortlisted' THEN 1 ELSE 0 END) AS is_shortlisted,
    MAX(CASE WHEN last_status_update IN ('Students Answered', 'Students Interested') THEN 1 ELSE 0 END) AS is_students_answered,
    MAX(CASE WHEN last_status_update = 'Students Interested' THEN 1 ELSE 0 END) AS is_students_interested,
    MAX(CASE WHEN last_status_update IN ('Companies Answered', 'Companies Approved') THEN 1 ELSE 0 END) AS is_companies_answered,
    MAX(CASE WHEN last_status_update = 'Companies Approved' THEN 1 ELSE 0 END) AS is_companies_approved
  FROM {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }}
  GROUP BY user_id
),
Shortlisted AS (
  SELECT user_id, 1 AS is_shortlisted
  FROM {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }}
  GROUP BY user_id  -- 
)
SELECT
  o.user_id,
  COALESCE(o.is_optin, 0) AS is_optin,
  COALESCE(o.is_optin_qualified, 0) AS is_optin_qualified,
  COALESCE(s.is_shortlisted, 0) AS is_shortlisted,
  COALESCE(c.is_students_answered, 0) AS is_students_answered,
  COALESCE(c.is_students_interested, 0) AS is_students_interested,
  COALESCE(c.is_companies_answered, 0) AS is_companies_answered,
  COALESCE(c.is_companies_approved, 0) AS is_companies_approved
FROM Optin o
FULL OUTER JOIN Shortlisted s ON o.user_id = s.user_id
FULL OUTER JOIN Candidate c ON o.user_id = c.user_id;