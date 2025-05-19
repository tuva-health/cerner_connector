select
    cast(name_last_key_a_nls as {{ dbt.type_string() }}) as name_last_key_a_nls
    , cast(name_first_key_a_nls as {{ dbt.type_string() }}) as name_first_key_a_nls
    , cast(person_id as {{ dbt.type_int() }}) as person_id
    , cast(updt_cnt as {{ dbt.type_int() }}) as updt_cnt
    , cast(updt_dt_tm as {{ dbt.type_timestamp() }}) as updt_dt_tm
    , cast(updt_id as {{ dbt.type_int() }}) as updt_id
    , cast(updt_task as {{ dbt.type_int() }}) as updt_task
    , cast(updt_applctx as {{ dbt.type_int() }}) as updt_applctx
    , cast(active_ind as {{ dbt.type_int() }}) as active_ind
    , cast(active_status_cd as {{ dbt.type_int() }}) as active_status_cd
    , cast(active_status_dt_tm as {{ dbt.type_timestamp() }}) as active_status_dt_tm
    , cast(active_status_prsnl_id as {{ dbt.type_int() }}) as active_status_prsnl_id
    , cast(create_prsnl_id as {{ dbt.type_int() }}) as create_prsnl_id
    , cast(create_dt_tm as {{ dbt.type_timestamp() }}) as create_dt_tm
    , cast(name_last_key as {{ dbt.type_string() }}) as name_last_key
    , cast(name_first_key as {{ dbt.type_string() }}) as name_first_key
    , cast(prsnl_type_cd as {{ dbt.type_int() }}) as prsnl_type_cd
    , cast(name_full_formatted as {{ dbt.type_string() }}) as name_full_formatted
    , cast(password as {{ dbt.type_string() }}) as password
    , cast(email as {{ dbt.type_string() }}) as email
    , cast(physician_ind as {{ dbt.type_int() }}) as physician_ind
    , cast(position_cd as {{ dbt.type_int() }}) as position_cd
    , cast(department_cd as {{ dbt.type_int() }}) as department_cd 
    , cast(free_text_ind as {{ dbt.type_int() }}) as free_text_ind
    , cast(beg_effective_dt_tm as {{ dbt.type_timestamp() }}) as beg_effective_dt_tm
    , cast(end_effective_dt_tm as {{ dbt.type_timestamp() }}) as end_effective_dt_tm
    , cast(section_cd as {{ dbt.type_int() }}) as section_cd
    , cast(data_status_cd as {{ dbt.type_int() }}) as data_status_cd
    , cast(data_status_dt_tm as {{ dbt.type_timestamp() }}) as data_status_dt_tm
    , cast(data_status_prsnl_id as {{ dbt.type_int() }}) as data_status_prsnl_id
    , cast(contributor_system_cd as {{ dbt.type_int() }}) as contributor_system_cd
    , cast(name_last as {{ dbt.type_string() }}) as name_last
    , cast(name_first as {{ dbt.type_string() }}) as name_first
    , cast(username as {{ dbt.type_string() }}) as username
    , cast(ft_entity_name as {{ dbt.type_string() }}) as ft_entity_name
    , cast(ft_entity_id as {{ dbt.type_int() }}) as ft_entity_id
    , cast(prim_assign_loc_cd as {{ dbt.type_int() }}) as prim_assign_loc_cd
    , cast(log_access_ind as {{ dbt.type_int() }}) as log_access_ind
    , cast(log_level as {{ dbt.type_int() }}) as log_level
    , cast(name_last_key_nls as {{ dbt.type_string() }}) as name_last_key_nls
    , cast(name_first_key_nls as {{ dbt.type_string() }}) as name_first_key_nls
    , cast(physician_status_cd as {{ dbt.type_int() }}) as physician_status_cd
    , cast(logical_domain_grp_id as {{ dbt.type_int() }}) as logical_domain_grp_id
    , cast(logical_domain_id as {{ dbt.type_int() }}) as logical_domain_id
from {{ source('cerner_raw', 'person_prsnl') }}
where active_ind = 1
and end_effective_dt_tm > current_timestamp()
