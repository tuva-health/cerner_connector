select    
    cast(org_name_key_a_nls as {{ dbt.type_string() }}) as org_name_key_a_nls
    , cast(organization_id as {{ dbt.type_int() }}) as organization_id
    , cast(updt_cnt as {{ dbt.type_int() }}) as updt_cnt
    , cast(updt_dt_tm as {{ dbt.type_timestamp() }}) as updt_dt_tm
    , cast(updt_id as {{ dbt.type_int() }}) as updt_id
    , cast(updt_task as {{ dbt.type_int() }}) as updt_task
    , cast(updt_applctx as {{ dbt.type_int() }}) as updt_applctx
    , cast(active_ind as {{ dbt.type_int() }}) as active_ind
    , cast(active_status_cd as {{ dbt.type_int() }}) as active_status_cd
    , cast(active_status_dt_tm as {{ dbt.type_timestamp() }}) as active_status_dt_tm
    , cast(active_status_prsnl_id as {{ dbt.type_int() }}) as active_status_prsnl_id
    , cast(beg_effective_dt_tm as {{ dbt.type_timestamp() }}) as beg_effective_dt_tm
    , cast(end_effective_dt_tm as {{ dbt.type_timestamp() }}) as end_effective_dt_tm
    , cast(data_status_cd as {{ dbt.type_int() }}) as data_status_cd
    , cast(data_status_dt_tm as {{ dbt.type_timestamp() }}) as data_status_dt_tm
    , cast(data_status_prsnl_id as {{ dbt.type_int() }}) as data_status_prsnl_id
    , cast(contributor_system_cd as {{ dbt.type_int() }}) as contributor_system_cd
    , cast(org_name as {{ dbt.type_string() }}) as org_name
    , cast(org_name_key as {{ dbt.type_string() }}) as org_name_key
    , cast(federal_tax_id_nbr as {{ dbt.type_string() }}) as federal_tax_id_nbr
    , cast(org_status_cd as {{ dbt.type_int() }}) as org_status_cd
    , cast(ft_entity_id as {{ dbt.type_int() }}) as ft_entity_id
    , cast(ft_entity_name as {{ dbt.type_string() }}) as ft_entity_name
    , cast(org_class_cd as {{ dbt.type_int() }}) as org_class_cd
    , cast(org_name_key_nls as {{ dbt.type_string() }}) as org_name_key_nls
    , cast(contributor_source_cd as {{ dbt.type_int() }}) as contributor_source_cd
    , cast(logical_domain_id as {{ dbt.type_int() }}) as logical_domain_id
from {{ source('cerner_raw', 'organization') }}
where active_ind = 1
and end_effective_dt_tm > current_timestamp()
