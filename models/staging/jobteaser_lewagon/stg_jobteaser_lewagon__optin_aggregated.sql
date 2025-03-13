with 

source as (

    select * from {{ source('jobteaser_lewagon', 'optin_aggregated') }}

),

renamed as (

    select
        user_id,
        last_receive_time,
        optin_date,
        optout_date,
        last_opt_status,
        last_cause,
        school_id,
        last_current_sign_in_at,
        last_resume_uploaded

    from source

)

select * from renamed
