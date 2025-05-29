with mapped_data as (
select
      d.diagnosis_id as condition_id
    , d.person_id as person_id
    , d.person_id as patient_id
    , d.encounter_id as encounter_id
    , d.diag_dt_tm as recorded_date
    , d.diag_dt_tm as onset_date
    , cast(null as {{ dbt.type_string() }}) as status
    , cast(null as {{ dbt.type_string() }}) as condition_type
    -- some additional mapping likely needed here
    , n.source_vocabulary_cd as source_code_type
    , n.source_identifier as source_code
    , d.diagnosis_display as source_description
    , d.clinical_diag_priority as condition_rank
    , d.present_on_admit_cd as present_on_admit_code
    , null as present_on_admit_description
    , 'Cerner' as data_source
from {{ ref('stg_cerner_diagnosis') }} as d
left join {{ ref('stg_cerner_nomenclature') }} as n
    on d.nomenclature_id = n.nomenclature_id
)

select
      cast(condition_id as {{ dbt.type_string() }}) as condition_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(null as {{ dbt.type_string() }}) as claim_id
    , cast(recorded_date as date) as recorded_date
    , cast(onset_date as date) as onset_date
    , cast(null as date) as resolved_date
    , cast(null as {{ dbt.type_string() }}) as status
    , cast(null as {{ dbt.type_string() }}) as condition_type
    , cast(source_code_type as {{ dbt.type_string() }}) as source_code_type
    , cast(source_code as {{ dbt.type_string() }}) as source_code
    , cast(source_description as {{ dbt.type_string() }}) as source_description
    , cast(null as {{ dbt.type_string() }}) as normalized_code_type
    , cast(null as {{ dbt.type_string() }}) as normalized_code
    , cast(null as {{ dbt.type_string() }}) as normalized_description
    , cast(condition_rank as {{ dbt.type_int() }}) as condition_rank
    , cast(present_on_admit_code as {{ dbt.type_string() }}) as present_on_admit_code
    , cast(null as {{ dbt.type_string() }}) as present_on_admit_description
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from mapped_data
