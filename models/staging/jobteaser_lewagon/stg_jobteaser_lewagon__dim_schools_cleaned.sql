with 

source as (

    select * from {{ source('jobteaser_lewagon', 'dim_schools_cleaned') }}

),

renamed as (

    select
        school_id,
        is_cc,
        intranet_school_id_clean,
        jt_country,
        jt_intranet_status_clean,
        jt_school_type

    from source

)

select * from renamed
