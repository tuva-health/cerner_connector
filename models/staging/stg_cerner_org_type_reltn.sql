select
   , cast(organization_id as {{ dbt.type_int() }}) as organization_id
   , cast(org_type_cd as {{ dbt.type_int() }}) as org_type_cd
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
from {{ source('cerner_raw', 'org_type_reltn') }}
where active_ind = 1
and end_effective_dt_tm > current_timestamp()
