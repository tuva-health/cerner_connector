select
  cast(unrounded_dose_qty as {{ dbt.type_float() }}) as unrounded_dose_qty
, cast(strength_with_overfill_value as {{ dbt.type_float() }}) as strength_with_overfill_value
, cast(strength_with_overfill_unit_cd as {{ dbt.type_int() }}) as strength_with_overfill_unit_cd
, cast(volume_with_overfill_value as {{ dbt.type_float() }}) as volume_with_overfill_value
, cast(volume_with_overfill_unit_cd as {{ dbt.type_int() }}) as volume_with_overfill_unit_cd
, cast(dose_quantity_txt as {{ dbt.type_string() }}) as dose_quantity_txt
, cast(pbs_drug_uuid as {{ dbt.type_string() }}) as pbs_drug_uuid
, cast(rxa_ordering_unit_cd as {{ dbt.type_int() }}) as rxa_ordering_unit_cd
, cast(item_id as {{ dbt.type_int() }}) as item_id
, cast(order_id as {{ dbt.type_int() }}) as order_id
, cast(action_sequence as {{ dbt.type_int() }}) as action_sequence
, cast(ingred_sequence as {{ dbt.type_int() }}) as ingred_sequence
, cast(tnf_id as {{ dbt.type_int() }}) as tnf_id
, cast(dose_quantity as {{ dbt.type_float() }}) as dose_quantity
, cast(dose_quantity_unit_cd as {{ dbt.type_int() }}) as dose_quantity_unit_cd
, cast(total_dispense_quantity as {{ dbt.type_float() }}) as total_dispense_quantity
, cast(total_charge_quantity as {{ dbt.type_float() }}) as total_charge_quantity
, cast(total_credit_quantity as {{ dbt.type_float() }}) as total_credit_quantity
, cast(updt_dt_tm as {{ dbt.type_timestamp() }}) as updt_dt_tm
, cast(updt_id as {{ dbt.type_int() }}) as updt_id
, cast(updt_task as {{ dbt.type_int() }}) as updt_task
, cast(updt_cnt as {{ dbt.type_int() }}) as updt_cnt
, cast(updt_applctx as {{ dbt.type_int() }}) as updt_applctx
, cast(iv_seq as {{ dbt.type_int() }}) as iv_seq
, cast(package_type_id as {{ dbt.type_int() }}) as package_type_id
, cast(med_product_id as {{ dbt.type_int() }}) as med_product_id
, cast(ignore_ind as {{ dbt.type_int() }}) as ignore_ind
, cast(compound_flag as {{ dbt.type_int() }}) as compound_flag
, cast(cmpd_base_ind as {{ dbt.type_int() }}) as cmpd_base_ind
, cast(product_seq as {{ dbt.type_int() }}) as product_seq
, cast(parent_product_seq as {{ dbt.type_int() }}) as parent_product_seq
, cast(label_desc as {{ dbt.type_string() }}) as label_desc
, cast(brand_desc as {{ dbt.type_string() }}) as brand_desc
, cast(generic_desc as {{ dbt.type_string() }}) as generic_desc
, cast(drug_identifier as {{ dbt.type_string() }}) as drug_identifier
, cast(disp_qty as {{ dbt.type_float() }}) as disp_qty
, cast(disp_qty_unit_cd as {{ dbt.type_float() }}) as disp_qty_unit_cd
, cast(auto_assign_flag as {{ dbt.type_int() }}) as auto_assign_flag
, cast(on_admin_ind as {{ dbt.type_int() }}) as on_admin_ind
from {{ source('cerner_raw', 'order_product') }}
where active_ind = 1
and end_effective_dt_tm > current_timestamp()
