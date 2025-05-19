with mapped_data as (
    select
        ce.clinical_event_id as observation_id
        , ce.person_id
        , ce.person_id as patient_id
        , ce.encounter_id
        , null as panel_id
        , ce.event_start_dt_tm as observation_date
        , ec.event_cd_descr as observation_type
        , oc.source_vocab_mean as source_code_type
        , cv.display as source_code
        , cv.description as source_code_description
        , cr.display as result
        , cv.display as source_units
        , ce.normal_low as source_reference_range_low
        , ce.normal_high as source_reference_range_high
        , 'Cerner' as data_source
    from {{ ref('stg_cerner_clinical_event') }} as ce
    /*
    Result units noted as code set 54
    https://docs.healtheintent.com/feed_types/millennium-ods/v1/#ce_calculation_result
    */
    left join {{ ref('stg_cerner_code_value') }} as cv
        on ce.result_units_cd = cv.code_value
        and cv.code_set = 54
    left join {{ ref('stg_cerner_code_value') }} as co
        on o.catalog_cd = co.code_value
    /*
    ordered procedure code values are stored on this catalog according to data model documentation:
    https://docs.healtheintent.com/feed_types/millennium-ods/v1/#mic_group_response
    */
        and o.code_set = 200
    left join {{ ref('stg_cerner_code_value') }} as cr
        on res.result_status_cd = cr.code_value
        and cv.code_set = 1901
    left join {{ ref('stg_cerner_v500_event_code') }} as ec
        on ce.event_cd = ec.event_cd
    left join {{ ref('stg_cerner_order_catalog') }} as oc
        on ce.catalog_cd = oc.catalog_cd
    left join {{ ref('stg_cerner_result') }} as res
        on res.event_id = ce.clinical_event_id
    where res.event_id is null
 )

select
      cast(observation_id as {{ dbt.type_string() }}) as observation_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(panel_id as {{ dbt.type_string() }}) as panel_id
    , cast(observation_date as date) as observation_date
    , cast(observation_type as {{ dbt.type_string() }}) as observation_type
    , cast(source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(source_code as {{ dbt.type_string() }}) as source_code
    , cast(source_description as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as normalized_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_code
    , cast(null as {{ dbt.type_string() }}) as normalized_description
    , cast(result as {{ dbt.type_string() }}) as result
    , cast(source_units as {{ dbt.type_string() }}) as source_units
    , cast(null as {{ dbt.type_string() }}) as normalized_units
    , cast(source_reference_range_low as {{ dbt.type_string() }}) as source_reference_range_low
    , cast(source_reference_range_high as {{ dbt.type_string() }}) as source_reference_range_high
    , cast(null as {{ dbt.type_string() }}) as normalized_reference_range_low
    , cast(null as {{ dbt.type_string() }}) as normalized_reference_range_high
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from mapped_data
