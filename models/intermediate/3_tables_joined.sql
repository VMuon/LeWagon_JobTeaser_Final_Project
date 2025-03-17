

SELECT 
    candidate_status.*,
    schools.school_id_master,
    schools.is_cc, 
    schools.intranet_school_id_clean,
    schools.jt_country,
    schools.jt_intranet_status_clean,
    schools.jt_school_type,
    optin.last_receive_time as optin_last_receive_time,
    optin.optin_date,
    optin.optout_date,
    optin.last_opt_status,
    optin.last_cause as optin_last_cause,
    optin.last_current_sign_in_at as optin_last_current_sign_in_at,
    optin.last_resume_uploaded
FROM {{ ref('stg_jobteaser_lewagon__candidate_status_aggregated') }} as candidate_status
LEFT JOIN {{ ref('stg_jobteaser_lewagon_dim_schools_cc_lvl') }} as schools
USING (school_id)
LEFT JOIN {{ ref('stg_jobteaser_lewagon__optin_aggregated') }} as optin 
USING (user_id)