select
    cast(code_value as {{ dbt.type_int() }}) as code_value
    , cast(code_set as {{ dbt.type_int() }}) as code_set
    , cast(cdf_meaning as {{ dbt.type_string() }}) as cdf_meaning
    , cast(display as {{ dbt.type_string() }}) as display
    , cast(display_key as {{ dbt.type_string() }}) as display_key
    , cast(description as {{ dbt.type_string() }}) as description
    , cast(definition as {{ dbt.type_string() }}) as definition
    , cast(collation_seq as {{ dbt.type_int() }}) as collation_seq
    , cast(active_type_cd as {{ dbt.type_int() }}) as active_type_cd
    , cast(active_ind as {{ dbt.type_int() }}) as active_ind
    , cast(active_dt_tm as {{ dbt.type_timestamp() }}) as active_dt_tm
    , cast(inactive_dt_tm as {{ dbt.type_timestamp() }}) as inactive_dt_tm
    , cast(updt_dt_tm as {{ dbt.type_timestamp() }}) as updt_dt_tm
    , cast(updt_id as {{ dbt.type_int() }}) as updt_id
    , cast(updt_cnt as {{ dbt.type_int() }}) as updt_cnt
    , cast(updt_task as {{ dbt.type_int() }}) as updt_task
    , cast(updt_applctx as {{ dbt.type_int() }}) as updt_applctx
    , cast(begin_effective_dt_tm as {{ dbt.type_timestamp() }}) 
        as begin_effective_dt_tm
    , cast(end_effective_dt_tm as {{ dbt.type_timestamp() }}) 
        as end_effective_dt_tm
    , cast(data_status_cd as {{ dbt.type_int() }}) as data_status_cd
    , cast(data_status_dt_tm as {{ dbt.type_timestamp() }}) 
        as data_status_dt_tm
    , cast(data_status_prsnl_id as {{ dbt.type_int() }}) 
        as data_status_prsnl_id
    , cast(active_status_prsnl_id as {{ dbt.type_int() }}) 
        as active_status_prsnl_id
    , cast(cki as {{ dbt.type_string() }}) as cki
    , cast(display_key_nls as {{ dbt.type_string() }}) as display_key_nls
    , cast(concept_cki as {{ dbt.type_string() }}) as concept_cki
    , cast(display_key_a_nls as {{ dbt.type_string() }}) as display_key_a_nls
from {{ source('cerner_raw', 'person') }}
