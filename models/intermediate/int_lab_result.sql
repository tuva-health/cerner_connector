/*
Notes:
Additional mapping needed to get to a source code/type/description
*/
with base as (
    select
      res.result_id as lab_result_id
      , ce.person_id as person_id
      , ce.person_id as patient_id
      , ce.encntr_id as encounter_id
      , ce.accession_nbr as accession_number
      , null as source_code_type
      , null as source_code
      , null as source_description
      , null as source_component
      , res.result_status_cd as status
      , cv.display as result
      , ce.event_end_dt_tm as result_date
      , ce.performed_dt_tm as collection_date
      , cu.display as source_units
      , ce.normal_low as source_reference_range_low
      , ce.normal_high as source_reference_range_high
from {{ ref('stg_cerner_result') }} as res
left join {{ ref('stg_cerner_clinical_event') }} as ce
  on res.event_id = ce.clinical_event_id
/*
Result status code noted as code set 1901:
https://docs.healtheintent.com/feed_types/millennium-ods/v1/
https://www.health.mil/Reference-Center/Technical-Documents/2024/12/03/MDR-Future-GENESIS-Microbiology-Results
*/
left join {{ ref('stg_cerner_code_value') }} as cv
  on res.result_status_cd = cv.code_value
  and cv.code_set = 1901
/*
Result measurement units noted as code set 54: 
https://docs.healtheintent.com/feed_types/millennium-ods/v1/#ce_calculation_result
*/
left join {{ ref('stg_cerner_code_value') }} as cu
  on ce.result_units_cd = cu.code_value
  and cu.code_set = 54
)


select
      cast(lab_result_id as {{ dbt.type_string() }}) as lab_result_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(accession_number as {{ dbt.type_string() }}) as accession_number
    , cast(source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(source_code as {{ dbt.type_string() }}) as source_code
    , cast(source_description as {{ dbt.type_string() }}) as source_description
    , cast(source_component as {{ dbt.type_string() }}) as source_component
    , cast(null as {{ dbt.type_string() }}) as normalized_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_code
    , cast(null as {{ dbt.type_string() }}) as normalized_description
    , cast(null as {{ dbt.type_string() }}) as normalized_component
    , cast(status as {{ dbt.type_string() }}) as status
    , cast(result as {{ dbt.type_string() }}) as result
    , cast(result_date as date) as result_date
    , cast(collection_date as date) as collection_date
    , cast(source_units as {{ dbt.type_string() }}) as source_units
    , cast(null as {{ dbt.type_string() }}) as normalized_units
    , cast(source_reference_range_low as {{ dbt.type_string() }}) as source_reference_range_low
    , cast(source_reference_range_high as {{ dbt.type_string() }}) as source_reference_range_high
    , cast(null as {{ dbt.type_string() }}) as normalized_reference_range_low
    , cast(null as {{ dbt.type_string() }}) as normalized_reference_range_high
    , cast(null as {{ dbt.type_int() }}) as source_abnormal_flag
    , cast(null as {{ dbt.type_int() }}) as normalized_abnormal_flag
    , cast(null as {{ dbt.type_string() }}) as specimen
    , cast(null as {{ dbt.type_string() }}) as ordering_practitioner_id
    , cast(null as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from base
