with ordering_provider as (
    select distinct
        order_id
        , ordering_provider_id
    from {{ ref('stg_cerner_order_action') }}
), 

mapped_data as (
    select
     ord.order_id as medication_id
    , ord.person_id as person_id
    , ord.person_id as patient_id
    , ord.encntr_id as encounter_id
    , ord.start_dispense_dt_tm as dispensing_date
    , o.orig_order_dt_tm as prescribing_date
    , op.drug_identifier as source_code
    , op.label_desc as source_description
    , o.unverified_route_cd as route
    , op.strength_with_overfill_value as strength
    , op.total_dispense_quantity as quantity
    -- need to map this unit code to a code_value
    -- , op.dosequantity_unit_cd as quantity_unit
    , ord.days_supply
    , opr.ordering_provider_id as practitioner_id
    , 'Cerner' as data_source
    from {{ ref('stg_cerner_order_dispense') }} as ord
    inner join {{ ref('stg_cerner_order') }} as o
        on ord.order_id = o.order_id
    left join {{ ref('stg_cerner_order_product') }} as op
        on ord.order_id = op.order_id
    left join ordering_provider as opr
        on ord.order_id = opr.order_id
)

select
      cast(medication_id as {{ dbt.type_string() }}) as medication_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(dispensing_date as date) as dispensing_date
    , cast(prescribing_date as date) as prescribing_date
    , cast(null as {{ dbt.type_string() }}) as source_code_type
    , cast(source_code as {{ dbt.type_string() }}) as source_code
    , cast(source_description as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as ndc_code
    , cast(null as {{ dbt.type_string() }}) as ndc_description
    , cast(null as {{ dbt.type_string() }}) as rxnorm_code
    , cast(null as {{ dbt.type_string() }}) as rxnorm_description
    , cast(null as {{ dbt.type_string() }}) as atc_code
    , cast(null as {{ dbt.type_string() }}) as atc_description
    , cast(route as {{ dbt.type_string() }}) as route
    , cast(strength as {{ dbt.type_string() }}) as strength
    , cast(quantity as {{ dbt.type_int() }}) as quantity
    , cast(null as {{ dbt.type_string() }}) as quantity_unit
    , cast(days_supply as {{ dbt.type_int() }}) as days_supply
    , cast(practitioner_id as {{ dbt.type_string() }}) as practitioner_id
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from mapped_data
