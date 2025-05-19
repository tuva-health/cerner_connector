with code_values as (
    select *
    from {{ ref('stg_cerner_code_value') }}
),

with ordering_provider as (
    select distinct
        order_id
        , ordering_provider_id
    from {{ ref('stg_cerner_order_action') }}
), 

mapped_data as (
    select
      o.order_id as procedure_id
    , o.person_id
    , o.person_id as patient_id
    , o.encounter_id
    , o.orig_order_dt_tm as procedure_date
    , cv.display as source_code
    , oc.source_vocab_mean as source_code_type,
    , cv.description as source_description
    , op.ordering_provider_id as practitioner_id
    , 'Cerner' as data_source
    from {{ ref('stg_cerner_orders') }} as o
    inner join code_values as cv
    /*
    ordered procedure code values are stored on this catalog according to data model documentation:
    https://docs.healtheintent.com/feed_types/millennium-ods/v1/#mic_group_response
    */
        on o.catalog_cd = cv.code_value
        and o.code_set = 200
    left join {{ ref('stg_cerner_order_catalog') }} as oc
        on o.catalog_cd = oc.catalog_cd
    left join ordering_provider as op
        on o.order_id = op.order_id
)

select
      cast(procedure_id as {{ dbt.type_string() }}) as procedure_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(null as {{ dbt.type_string() }}) as claim_id
    , cast(procedure_date as date) as procedure_date
    , cast(source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(source_code as {{ dbt.type_string() }}) as source_code
    , cast(source_description as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as normalized_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_code
    , cast(null as {{ dbt.type_string() }}) as normalized_description
    , cast(null as {{ dbt.type_string() }}) as modifier_1
    , cast(null as {{ dbt.type_string() }}) as modifier_2
    , cast(null as {{ dbt.type_string() }}) as modifier_3
    , cast(null as {{ dbt.type_string() }}) as modifier_4
    , cast(null as {{ dbt.type_string() }}) as modifier_5
    , cast(practitioner_id as {{ dbt.type_string() }}) as practitioner_id
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from mapped_data
