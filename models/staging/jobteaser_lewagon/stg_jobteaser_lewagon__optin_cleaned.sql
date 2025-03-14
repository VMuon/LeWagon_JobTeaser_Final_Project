with 

source as (

    select * from {{ source('jobteaser_lewagon', 'optin_cleaned') }}

),

renamed as (

    select
        index,
        user_id,
        receive_time,
        cause,
        active,
        school_id,
        current_sign_in_at,
        resume_uploaded

    from source

)

select * from renamed
