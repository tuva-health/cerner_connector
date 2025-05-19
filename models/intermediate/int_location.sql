with address_info as (
    select
        a.parent_entity_id
        , a.street_addr as address
        , a.city
        , a.state
        , a.zipcode
    from {{ ref('stg_cerner_address') }} as a
    -- todo: add comment
    left join {{ ref('stg_cerner_code_value') }} as v
        on a.address_type_cd = v.code_value
        and v.code_set = 212
    where lower(parent_entity_name) = 'organization'
    and v.display ilike ('%primary%')
    qualify row_number() over 
        (partition by parent_entity_id 
            order by address_type_seq asc) = 1
),

mapped_data as (
    select
        o.organization_id as location_id
        , o.org_name as name
        , ad.address
        , ad.state
        , ad.zipcode as zip_code
        , 'Cerner' as data_source
    from {{ ref('stg_cerner_organization') }} as o
    left join address_info as ad
        on o.organization_id = ad.parent_entity_id
)

select      
    cast(location_id as {{ dbt.type_string() }}) as location_id
    , cast(null as {{ dbt.type_string() }}) as npi
    , cast(name as {{ dbt.type_string() }}) as name
    , cast(null as {{ dbt.type_string() }}) as facility_type
    , cast(null as {{ dbt.type_string() }}) as parent_organization
    , cast(address as {{ dbt.type_string() }}) as address
    , cast(city as {{ dbt.type_string() }}) as city
    , cast(state as {{ dbt.type_string() }}) as state
    , cast(zip_code as {{ dbt.type_string() }}) as zip_code
    , cast(null as {{ dbt.type_float() }}) as latitude
    , cast(null as {{ dbt.type_float() }}) as longitude
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from mapped_data
