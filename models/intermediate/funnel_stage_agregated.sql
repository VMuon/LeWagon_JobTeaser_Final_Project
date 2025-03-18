###funnel stage_aggregated
SELECT
  'Opt-in' AS stage, SUM(is_optin) AS user_count, 1 AS stage_order
FROM `{{ ref('tunnel_stage') }} `
UNION ALL
SELECT 'Opt-in Qualified', SUM(is_optin_qualified), 2
FROM {{ ref('tunnel_stage') }}
UNION ALL
SELECT 'Shortlisted', COUNT(DISTINCT is_shortlisted), 3
FROM {{ ref('tunnel_stage') }}
WHERE is_shortlisted = 1
UNION ALL
SELECT 'Students Answered', SUM(is_students_answered), 4
FROM {{ ref('tunnel_stage') }}
UNION ALL
SELECT 'Students Interested', SUM(is_students_interested), 5
FROM {{ ref('tunnel_stage') }}
UNION ALL
SELECT 'Companies Answered', SUM(is_companies_answered), 6
UNION ALL
FROM {{ ref('tunnel_stage') }}
SELECT 'Companies Approved', SUM(is_companies_approved), 7
FROM {{ ref('tunnel_stage') }}
ORDER BY stage_order;

