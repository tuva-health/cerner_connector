select
 cast(result_id as {{ dbt.type_int() }}) as result_id
, cast(order_id as {{ dbt.type_int() }}) as order_id
, cast(call_back_ind as {{ dbt.type_int() }}) as call_back_ind
, cast(catalog_cd as {{ dbt.type_int() }}) as catalog_cd
, cast(task_assay_cd as {{ dbt.type_int() }}) as task_assay_cd
, cast(result_status_cd as {{ dbt.type_int() }}) as result_status_cd
, cast(chartable_flag as {{ dbt.type_int() }}) as chartable_flag
, cast(security_level_cd as {{ dbt.type_int() }}) as security_level_cd
, cast(repeat_number as {{ dbt.type_int() }}) as repeat_number
, cast(comment_ind as {{ dbt.type_int() }}) as comment_ind
, cast(nc_comment_ind as {{ dbt.type_int() }}) as nc_comment_ind
, cast(updt_dt_tm as {{ dbt.type_timestamp() }}) as updt_dt_tm
, cast(updt_id as {{ dbt.type_int() }}) as updt_id
, cast(updt_task as {{ dbt.type_int() }}) as updt_task
, cast(updt_cnt as {{ dbt.type_int() }}) as updt_cnt
, cast(updt_applctx as {{ dbt.type_int() }}) as updt_applctx
, cast(bb_result_id as {{ dbt.type_int() }}) as bb_result_id
, cast(event_id as {{ dbt.type_int() }}) as event_id
, cast(bb_control_cell_cd as {{ dbt.type_int() }}) as bb_control_cell_cd
, cast(person_id as {{ dbt.type_int() }}) as person_id
, cast(biological_category_cd as {{ dbt.type_int() }}) as biological_category_cd
, cast(primary_ind as {{ dbt.type_int() }}) as primary_ind
, cast(display_sequence_nbr as {{ dbt.type_int() }}) as display_sequence_nbr
, cast(restrict_display_ind as {{ dbt.type_int() }}) as restrict_display_ind
, cast(pending_ind as {{ dbt.type_int() }}) as pending_ind
, cast(result_processing_cd as {{ dbt.type_int() }}) as result_processing_cd
, cast(concept_cki as {{ dbt.type_string() }}) as concept_cki
, cast(bb_group_id as {{ dbt.type_int() }}) as bb_group_id
, cast(lot_information_id as {{ dbt.type_int() }}) as lot_information_id
from {{ source('cerner_raw', 'result') }}
