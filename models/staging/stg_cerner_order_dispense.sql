select
    , cast(rxa_sig_override_ind as {{ dbt.type_int() }}) as rxa_sig_override_ind
    , cast(last_pbs_item_code as {{ dbt.type_string() }}) as last_pbs_item_code
    , cast(last_pbs_drug_uuid as {{ dbt.type_string() }}) as last_pbs_drug_uuid
    , cast(last_utc_ts as {{ dbt.type_timestamp() }}) as last_utc_ts
    , cast(order_id as {{ dbt.type_int() }}) as order_id
    , cast(next_dispense_dt_tm as {{ dbt.type_timestamp() }}) as next_dispense_dt_tm
    , cast(dispense_category_cd as {{ dbt.type_int() }}) as dispense_category_cd
    , cast(encntr_id as {{ dbt.type_int() }}) as encntr_id
    , cast(prn_ind as {{ dbt.type_int() }}) as prn_ind
    , cast(print_ind as {{ dbt.type_int() }}) as print_ind
    , cast(total_dispense_doses as {{ dbt.type_int() }}) as total_dispense_doses
    , cast(updt_dt_tm as {{ dbt.type_timestamp() }}) as updt_dt_tm
    , cast(updt_id as {{ dbt.type_int() }}) as updt_id
    , cast(updt_task as {{ dbt.type_int() }}) as updt_task
    , cast(updt_cnt as {{ dbt.type_int() }}) as updt_cnt
    , cast(updt_applctx as {{ dbt.type_int() }}) as updt_applctx
    , cast(next_iv_seq as {{ dbt.type_int() }}) as next_iv_seq
    , cast(floorstock_ind as {{ dbt.type_int() }}) as floorstock_ind
    , cast(cart_fill_doses1 as {{ dbt.type_int() }}) as cart_fill_doses1
    , cast(cart_fill_doses2 as {{ dbt.type_int() }}) as cart_fill_doses2
    , cast(cart_fill_doses3 as {{ dbt.type_int() }}) as cart_fill_doses3
    , cast(cart_fill_dt_tm1 as {{ dbt.type_timestamp() }}) as cart_fill_dt_tm1
    , cast(cart_fill_dt_tm2 as {{ dbt.type_timestamp() }}) as cart_fill_dt_tm2
    , cast(cart_fill_dt_tm3 as {{ dbt.type_timestamp() }}) as cart_fill_dt_tm3
    , cast(cart_fill_run_id1 as {{ dbt.type_int() }}) as cart_fill_run_id1
    , cast(cart_fill_run_id2 as {{ dbt.type_int() }}) as cart_fill_run_id2
    , cast(cart_fill_run_id3 as {{ dbt.type_int() }}) as cart_fill_run_id3
    , cast(dept_status_cd as {{ dbt.type_int() }}) as dept_status_cd
    , cast(display_line as {{ dbt.type_string() }}) as display_line
    , cast(floorstock_override_ind as {{ dbt.type_int() }}) as floorstock_override_ind
    , cast(frequency_id as {{ dbt.type_int() }}) as frequency_id
    , cast(ignore_ind as {{ dbt.type_int() }}) as ignore_ind
    , cast(iv_set_size as {{ dbt.type_int() }}) as iv_set_size
    , cast(last_ver_act_seq as {{ dbt.type_int() }}) as last_ver_act_seq
    , cast(last_ver_ingr_seq as {{ dbt.type_int() }}) as last_ver_ingr_seq
    , cast(order_cost_value as {{ dbt.type_int() }}) as order_cost_value
    , cast(order_dispense_ind as {{ dbt.type_int() }}) as order_dispense_ind
    , cast(order_price_value as {{ dbt.type_int() }}) as order_price_value
    , cast(order_type as {{ dbt.type_int() }}) as order_type
    , cast(par_doses as {{ dbt.type_int() }}) as par_doses
    , cast(price_schedule_id as {{ dbt.type_int() }}) as price_schedule_id
    , cast(replace_every as {{ dbt.type_int() }}) as replace_every
    , cast(replace_every_cd as {{ dbt.type_int() }}) as replace_every_cd
    , cast(resume_dt_tm as {{ dbt.type_timestamp() }}) as resume_dt_tm
    , cast(stop_dt_tm as {{ dbt.type_timestamp() }}) as stop_dt_tm
    , cast(stop_type_cd as {{ dbt.type_int() }}) as stop_type_cd
    , cast(suspend_dt_tm as {{ dbt.type_timestamp() }}) as suspend_dt_tm
    , cast(last_fill_status as {{ dbt.type_int() }}) as last_fill_status
    , cast(price_code_cd as {{ dbt.type_int() }}) as price_code_cd
    , cast(qty_remaining as {{ dbt.type_int() }}) as qty_remaining
    , cast(refills_remaining as {{ dbt.type_int() }}) as refills_remaining
    , cast(rx_nbr as {{ dbt.type_int() }}) as rx_nbr
    , cast(rx_nbr_cd as {{ dbt.type_int() }}) as rx_nbr_cd
    , cast(total_rx_qty as {{ dbt.type_int() }}) as total_rx_qty
    , cast(transfer_cnt as {{ dbt.type_int() }}) as transfer_cnt
    , cast(claim_flag as {{ dbt.type_int() }}) as claim_flag
    , cast(daw_cd as {{ dbt.type_int() }}) as daw_cd
    , cast(days_supply as {{ dbt.type_int() }}) as days_supply
    , cast(fill_nbr as {{ dbt.type_int() }}) as fill_nbr
    , cast(health_plan_id as {{ dbt.type_int() }}) as health_plan_id
    , cast(last_fill_dispense_hx_id as {{ dbt.type_int() }}) as last_fill_dispense_hx_id
    , cast(last_fill_hx_id as {{ dbt.type_int() }}) as last_fill_hx_id
    , cast(last_refill_dt_tm as {{ dbt.type_timestamp() }}) as last_refill_dt_tm
    , cast(legal_status_cd as {{ dbt.type_int() }}) as legal_status_cd
    , cast(person_id as {{ dbt.type_int() }}) as person_id
    , cast(pharm_type_cd as {{ dbt.type_int() }}) as pharm_type_cd
    , cast(last_fill_act_seq as {{ dbt.type_int() }}) as last_fill_act_seq
    , cast(last_fill_ingr_seq as {{ dbt.type_int() }}) as last_fill_ingr_seq
    , cast(expire_dt_tm as {{ dbt.type_timestamp() }}) as expire_dt_tm
    , cast(last_rx_dispense_hx_id as {{ dbt.type_int() }}) as last_rx_dispense_hx_id
    , cast(owe_qty as {{ dbt.type_int() }}) as owe_qty
    , cast(research_account_id as {{ dbt.type_int() }}) as research_account_id
    , cast(next_dispense_tz as {{ dbt.type_int() }}) as next_dispense_tz
    , cast(cart_fill1_tz as {{ dbt.type_int() }}) as cart_fill1_tz
    , cast(cart_fill2_tz as {{ dbt.type_int() }}) as cart_fill2_tz
    , cast(cart_fill3_tz as {{ dbt.type_int() }}) as cart_fill3_tz
    , cast(resume_tz as {{ dbt.type_int() }}) as resume_tz
    , cast(stop_tz as {{ dbt.type_int() }}) as stop_tz
    , cast(suspend_tz as {{ dbt.type_int() }}) as suspend_tz
    , cast(last_refill_tz as {{ dbt.type_int() }}) as last_refill_tz
    , cast(expire_tz as {{ dbt.type_int() }}) as expire_tz
    , cast(need_rx_verify_ind as {{ dbt.type_int() }}) as need_rx_verify_ind
    , cast(unverified_comm_type_cd as {{ dbt.type_int() }}) as unverified_comm_type_cd
    , cast(unverified_action_type_cd as {{ dbt.type_int() }}) as unverified_action_type_cd
    , cast(unverified_route_cd as {{ dbt.type_int() }}) as unverified_route_cd
    , cast(unverified_rx_ord_priority_cd as {{ dbt.type_int() }}) as unverified_rx_ord_priority_cd
    , cast(cob_ind as {{ dbt.type_int() }}) as cob_ind
    , cast(parent_order_id as {{ dbt.type_int() }}) as parent_order_id
    , cast(reviewed_parent_action_seq as {{ dbt.type_int() }}) as reviewed_parent_action_seq
    , cast(profile_display_dt_tm as {{ dbt.type_timestamp() }}) as profile_display_dt_tm
    , cast(source_parent_action_seq as {{ dbt.type_int() }}) as source_parent_action_seq
    , cast(future_loc_facility_cd as {{ dbt.type_int() }}) as future_loc_facility_cd
    , cast(future_loc_nurse_unit_cd as {{ dbt.type_int() }}) as future_loc_nurse_unit_cd
    , cast(need_rx_prod_assign_flag as {{ dbt.type_int() }}) as need_rx_prod_assign_flag
    , cast(last_clin_review_act_seq as {{ dbt.type_int() }}) as last_clin_review_act_seq
    , cast(last_clin_review_ingr_seq as {{ dbt.type_int() }}) as last_clin_review_ingr_seq
    , cast(workflow_cd as {{ dbt.type_int() }}) as workflow_cd
    , cast(start_dispense_dt_tm as {{ dbt.type_timestamp() }}) as start_dispense_dt_tm
    , cast(start_dispense_tz as {{ dbt.type_int() }}) as start_dispense_tz
    , cast(patient_med_ind as {{ dbt.type_int() }}) as patient_med_ind
    , cast(auto_credit_ind as {{ dbt.type_int() }}) as auto_credit_ind
from {{ source('cerner_raw', 'order_dispense') }} 
