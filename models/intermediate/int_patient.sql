with address_info as (
        select
        a.parent_entity_id
        , a.street_addr as address
        , a.city
        , a.state
        , a.zipcode
    from {{ ref('stg_cerner_address') }} as a
    /*
    Address type noted as code set 212
    https://docs.healtheintent.com/feed_types/millennium-ods/v1/#roi_request
    */
    left join {{ ref('stg_cerner_code_value') }} as v
        on a.address_type_cd = v.code_value
        and v.code_set = 212
    where lower(parent_entity_name) = 'person'
    and v.display ilike ('%home%')
    qualify row_number() over 
        (partition by parent_entity_id 
            order by address_type_seq asc) = 1
),

mapped_data as (
    select
        p.person_id as person_id
        , p.person_id as patient_id
        , p.name_first as first_name
        , p.name_last as last_name
        , cv.display as sex
        , null as race
        , birth_dt_tm as birth_date
        , deceased_dt_tm as death_date
        , case when deceased_dt_tm is not null then 1 else 0 end as death_flag
        , a.address
        , a.city
        , a.state
        , a.zipcode as zip_code
        , 'Cerner' as data_source
    from {{ ref('stg_cerner_person') }} as p
    /*
    documentation on this code set value:
    https://ulearn.cerner.com/content/cerner/courses/Discern_Explorer1/content/htmlFiles/contentPages/DVDev%20Participant%20Guide.htm
    */
    left join {{ ref('stg_cerner_code_value') }} as g
        on p.sex_cd = g.code_value
        and code_set = 57
    left join address_info as a
        on p.person_id = a.parent_entity_id
        and lower(parent_entity_id) = 'person'
    inner join {{ ref('stg_cerner_person_patient') }} as pt
        on p.person_id = pt.person_id
    
)

select
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(first_name as {{ dbt.type_string() }}) as first_name
    , cast(last_name as {{ dbt.type_string() }}) as last_name
    , cast(sex as {{ dbt.type_string() }}) as sex
    , cast(null as {{ dbt.type_string() }}) as race
    , cast(birth_date as date) as birth_date
    , cast(death_date as date) as death_date
    , cast(death_flag as {{ dbt.type_int() }}) as death_flag
    , cast(null as {{ dbt.type_string() }}) as social_security_number
    , cast(address as {{ dbt.type_string() }}) as address
    , cast(city as {{ dbt.type_string() }}) as city
    , cast(state as {{ dbt.type_string() }}) as state
    , cast(zip_code as {{ dbt.type_string() }}) as zip_code
    , cast(null as {{ dbt.type_string() }}) as county
    , cast(null as {{ dbt.type_float() }}) as latitude
    , cast(null as {{ dbt.type_float() }}) as longitude
    , cast(null as {{ dbt.type_string() }}) as phone
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from mapped_data
