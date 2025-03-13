with 

source as (

    select * 
    from {{ source('jobteaser_lewagon', 'candidate_status_update_cleaned') }} as status
    join {{ ref('stg_jobteaser_lewagon__optin_cleaned') }} as optin
    using (user_id)

),

renamed as (

    select
        user_id,
        status.receive_time,
        shortlist_id,
        status_update,
        cause,
        school_id,
        current_sign_in_at,
        next_receive_time,
        response_time_hours,
        

    from source

)

select *
from renamed
