with 

source as (

    select * from {{ source('jobteaser_lewagon', 'candidate_status_aggregated') }}

),

renamed as (

    select
        user_id,
        shortlist_id,
        last_receive_time,
        awaiting_date,
        approved_date,
        declined_date,
        interested_date,
        not_interested_date,
        last_status_update,
        last_cause,
        school_id,
        last_current_sign_in_at

    from source

)

select * from renamed
