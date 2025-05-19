with mapped_data as (
  select
    person_id as practitioner_id
    , name_first as first_name
    , name_last as last_name
    , 'Cerner' as data_source
  from {{ ref('stg_cerner_person_prsnl') }}
  where physician_ind = 1
)

select
      cast(practitioner_id as {{ dbt.type_string() }}) as practitioner_id
    , cast(null as {{ dbt.type_string() }}) as npi
    , cast(first_name as {{ dbt.type_string() }}) as first_name
    , cast(last_name as {{ dbt.type_string() }}) as last_name
    , cast(null as {{ dbt.type_string() }}) as practice_affiliation
    , cast(null as {{ dbt.type_string() }}) as specialty
    , cast(null as {{ dbt.type_string() }}) as sub_specialty
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from mapped_data
