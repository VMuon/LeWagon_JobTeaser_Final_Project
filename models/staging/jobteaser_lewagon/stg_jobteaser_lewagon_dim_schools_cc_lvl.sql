with 

source as (

    select * from {{ source('jobteaser_lewagon', 'dim_schools_cleaned') }}

),

renamed as (

    select
        school_id
        , is_cc
        , intranet_school_id_clean
        , CASE
            WHEN intranet_school_id_clean NOT IN ("is_a_Career_Center","has_no_Career_Center") THEN intranet_school_id_clean
            ELSE school_id
            END AS school_id_master
        , jt_country
        , jt_intranet_status_clean
        , jt_school_type

    from source

)

select * from renamed