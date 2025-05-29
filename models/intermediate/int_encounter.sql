/*
Notes:
1. Code values are foundational in how Cerner's data model works.
   The code_value table contains a number of different code sets; some
   are standardized and others are highly customizable.
   More information about code sets can be found here:
   https://docs.oracle.com/en/industries/health/millennium-platform-apis/mfrap/proprietary.html
   and in the data model documentation here: 
   https://docs.healtheintent.com/feed_types/millennium-ods/v1
2.  
*/
with code_values as (
    select *
    from {{ ref('stg_cerner_code_value') }}
),

mapped_data as (
    select
        enc.encntr_id as encounter_id
        , enc.person_id as person_id
        , enc.person_id as patient_id
        , cv.display as encounter_type
        , coalesce(enc.arrive_dt_tm, enc.reg_dt_tm) as encounter_start_date
        , coalesce(enc.disch_dt_tm, enc.depart_dt_tm) as encounter_end_date
        , enc.est_length_of_stay as length_of_stay
        , enc.admit_src_cd as admit_source_code
        , sc.display as admit_source_description
        , enc.admit_type_cd as admit_type_code
        , st.display as admit_type_description
        , enc.disch_disposition_cd as discharge_disposition_code
        , dd.display as discharge_disposition_description
        , enc.organization_id as facility_id
        , org.org_name as facility_name
        , 'Cerner' as data_source
        , null as file_name
    from {{ ref('stg_cerner_encounter') }} as enc
    /*
    Cerner data model defines code set 71 as the visit / encounter type:
    https://docs.healtheintent.com/feed_types/millennium-ods/v1/#interface_charge
    */
    left join code_values as cv
        on enc.encntr_type_cd = cv.code_value
        and cv.code_set = 71
    left join code_values as sc
    /*
    Cerner data model defines code set 2 as the admit source code. Noted in two locations:
    https://docs.healtheintent.com/feed_types/millennium-ods/v1/#lh_d_admit_src
    https://health.mil/Reference-Center/Technical-Documents/2024/10/22/MDR-Future-GENESIS-Encounter-Episodic-Table
    */
        on enc.admit_src_cd = sc.code_value
        and code_set = 2
    left join code_values as st
    /*
    In Cerner data model, this admit type code is noted as codeset 71:
    https://docs.healtheintent.com/feed_types/millennium-ods/v1/#interface_charge
    */
        on enc.admit_type_cd = st.code_value
        and code_set_id = 71
    /*
    Discharge disposition code is widely documented as code set 19.
    e.g. see page 8 here:
    https://health.mil/Reference-Center/Technical-Documents/2024/10/22/MDR-Future-GENESIS-Encounter-Episodic-Table
    */
    left join code_values as dd
        on enc.disch_disposition_cd = dd.code_value
        and code_set = 19
    left join {{ ref('stg_cerer_organization') }} as org
        on enc.organization_id = org.organization_id
)


select
      cast(encounter_id as {{ dbt.type_string() }}) as encounter_id
    , cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(encounter_type as {{ dbt.type_string() }}) as encounter_type
    , cast(encounter_start_date as date) as encounter_start_date
    , cast(encounter_end_date as date) as encounter_end_date
    , cast(length_of_stay as {{ dbt.type_int() }}) as length_of_stay
    , cast(admit_source_code as {{ dbt.type_string() }}) as admit_source_code
    , cast(admit_source_description as {{ dbt.type_string() }}) as admit_source_description
    , cast(admit_type_code as {{ dbt.type_string() }}) as admit_type_code
    , cast(admit_type_description as {{ dbt.type_string() }}) as admit_type_description
    , cast(discharge_disposition_code as {{ dbt.type_string() }}) as discharge_disposition_code
    , cast(discharge_disposition_description as {{ dbt.type_string() }}) as discharge_disposition_description
    , cast(null as {{ dbt.type_string() }}) as attending_provider_id
    , cast(null as {{ dbt.type_string() }}) as attending_provider_name
    , cast(facility_id as {{ dbt.type_string() }}) as facility_id
    , cast(facility_name as {{ dbt.type_string() }}) as facility_name
    , cast(null as {{ dbt.type_string() }}) as primary_diagnosis_code_type
    , cast(null as {{ dbt.type_string() }}) as primary_diagnosis_code
    , cast(null as {{ dbt.type_string() }}) as primary_diagnosis_description
    , cast(null as {{ dbt.type_string() }}) as drg_code_type
    , cast(null as {{ dbt.type_string() }}) as drg_code
    , cast(null as {{ dbt.type_string() }}) as drg_description
    , cast(null as {{ dbt.type_numeric() }}) as paid_amount
    , cast(null as {{ dbt.type_numeric() }}) as allowed_amount
    , cast(null as {{ dbt.type_numeric() }}) as charge_amount
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
from mapped_data
