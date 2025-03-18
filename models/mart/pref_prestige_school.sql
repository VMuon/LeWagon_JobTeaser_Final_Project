SELECT 
user_id
, shortlist_id
, last_receive_time AS hiring_date
, EXTRACT (YEAR FROM last_receive_time) AS hiring_year
, EXTRACT (MONTH FROM last_receive_time) AS hiring_month
, jt_school_type AS school_type
, last_status_update AS last_status
, CASE 
    WHEN jt_school_type = 1 THEN "Engineer Schools / TU"
    WHEN jt_school_type = 2 THEN "Business Schools / Business Universities"
    WHEN jt_school_type = 3 THEN "Other Universities"
    ELSE "Other"
  END AS school_category
, jt_country AS school_country

FROM {{ ref('3_tables_joined') }}
WHERE last_status_update = "approved"