select
    cast(city_cd as {{ dbt.type_int() }}) as city_cd
    , cast(validation_expire_dt_tm as {{ dbt.type_timestamp() }}) as validation_expire_dt_tm
    , cast(zipcode as {{ dbt.type_string() }}) as zipcode
    , cast(zip_code_group_cd as {{ dbt.type_int() }}) as zip_code_group_cd
    , cast(postal_barcode_info as {{ dbt.type_string() }}) as postal_barcode_info
    , cast(county as {{ dbt.type_string() }}) as county
    , cast(county_cd as {{ dbt.type_int() }}) as county_cd
    , cast(country as {{ dbt.type_string() }}) as country
    , cast(country_cd as {{ dbt.type_int() }}) as country_cd
    , cast(residence_cd as {{ dbt.type_int() }}) as country_cd
    , cast(mail_stop as {{ dbt.type_string() }}) as mail_stop
    , cast(data_status_cd as {{ dbt.type_int() }}) as data_status_cd
    , cast(data_status_dt_tm as {{ dbt.type_timestamp() }}) as data_status_dt_tm
    , cast(data_status_prsnl_id as {{ dbt.type_int() }}) as data_status_prsnl_id
    , cast(address_type_seq as {{ dbt.type_int() }}) as address_type_seq
    , cast(beg_effective_mm_dd as {{ dbt.type_int() }}) as beg_effective_mm_dd
    , cast(end_effective_mm_dd as {{ dbt.type_int() }}) as end_effective_mm_dd
    , cast(contributor_system_cd as {{ dbt.type_int() }}) as contributor_system_cd
    , cast(operation_hours as {{ dbt.type_string() }}) as operation_hours
    , cast(long_text_id as {{ dbt.type_int() }}) as long_text_id
    , cast(address_info_status_cd as {{ dbt.type_int() }}) as address_info_status_cd
    , cast(primary_care_cd as {{ dbt.type_int() }}) as primary_care_cd
    , cast(district_health_cd as {{ dbt.type_int() }}) as district_health_cd
    , cast(zipcode_key as {{ dbt.type_string() }}) as zipcode_key
    , cast(postal_identifier as {{ dbt.type_string() }}) as postal_identifier
    , cast(postal_identifier_key as {{ dbt.type_string() }}) as postal_identifier_key
    , cast(source_identifier as {{ dbt.type_string() }}) as source_identifier
    , cast(address_id as {{ dbt.type_int() }}) as address_id
    , cast(parent_entity_name as {{ dbt.type_string() }}) as parent_entity_name
    , cast(parent_entity_id as {{ dbt.type_int() }}) as parent_entity_id
    , cast(address_type_cd as {{ dbt.type_int() }}) as address_type_cd
    , cast(updt_cnt as {{ dbt.type_int() }}) as updt_cnt
    , cast(updt_dt_tm as {{ dbt.type_timestamp() }}) as updt_dt_tm
    , cast(updt_id as {{ dbt.type_int() }}) as updt_id
    , cast(updt_task as {{ dbt.type_int() }}) as updt_task
    , cast(updt_applctx as {{ dbt.type_int() }}) as updt_applctx
    , cast(active_ind as {{ dbt.type_int() }}) as active_ind
    , cast(active_status_cd as {{ dbt.type_int() }}) as active_status_cd
    , cast(active_status_dt_tm as {{ dbt.type_timestamp() }}) as active_status_dt_tm
    , cast(active_status_prsnl_id as {{ dbt.type_int() }}) as active_status_prsnl_id
    , cast(address_format_cd as {{ dbt.type_int() }}) as address_format_cd
    , cast(beg_effective_dt_tm as {{ dbt.type_timestamp() }}) as beg_effective_dt_tm
    , cast(end_effective_dt_tm as {{ dbt.type_timestamp() }}) as end_effective_dt_tm
    , cast(contact_name as {{ dbt.type_string() }}) as contact_name
    , cast(residence_type_cd as {{ dbt.type_int() }}) as residence_type_cd
    , cast(comment_txt as {{ dbt.type_string() }}) as comment_txt
    , cast(street_addr as {{ dbt.type_string() }}) as street_addr
    , cast(street_addr2 as {{ dbt.type_string() }}) as street_addr2
    , cast(street_addr3 as {{ dbt.type_string() }}) as street_addr3
    , cast(street_addr4 as {{ dbt.type_string() }}) as street_addr4
    , cast(city as {{ dbt.type_string() }}) as city
    , cast(state as {{ dbt.type_string() }}) as state
    , cast(state_cd as {{ dbt.type_int() }}) as state_cd
from {{ source('cerner_raw', 'address')  }}
where active_ind = 1
and end_effective_dt_tm > current_timestamp()
