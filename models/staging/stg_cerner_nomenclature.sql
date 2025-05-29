select
    cast(source_string_keycap_a_nls as {{ dbt.type_string() }}) as source_string_keycap_a_nls
    , cast(nomenclature_id as {{ dbt.type_int() }}) as nomenclature_id
    , cast(principle_type_cd as {{ dbt.type_int() }}) as principle_type_cd
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
    , cast(contributor_system_cd as {{ dbt.type_int() }}) as contributor_system_cd
    , cast(source_string as {{ dbt.type_string() }}) as source_string
    , cast(source_identifier as {{ dbt.type_string() }}) as source_identifier
    , cast(string_identifier as {{ dbt.type_string() }}) as string_identifier
    , cast(string_status_cd as {{ dbt.type_int() }}) as string_status_cd
    , cast(term_id as {{ dbt.type_int() }}) as term_id
    , cast(language_cd as {{ dbt.type_int() }}) as language_cd
    , cast(source_vocabulary_cd as {{ dbt.type_int() }}) as source_vocabulary_cd
    , cast(nom_ver_grp_id as {{ dbt.type_int() }}) as nom_ver_grp_id
    , cast(data_status_cd as {{ dbt.type_int() }}) as data_status_cd
    , cast(data_status_dt_tm as {{ dbt.type_timestamp() }}) as data_status_dt_tm
    , cast(data_status_prsnl_id as {{ dbt.type_int() }}) as data_status_prsnl_id
    , cast(short_string as {{ dbt.type_string() }}) as short_string
    , cast(mnemonic as {{ dbt.type_string() }}) as mnemonic
    , cast(concept_identifier as {{ dbt.type_string() }}) as concept_identifier
    , cast(concept_source_cd as {{ dbt.type_int() }}) as concept_source_cd
    , cast(string_source_cd as {{ dbt.type_int() }}) as string_source_cd
    , cast(vocab_axis_cd as {{ dbt.type_int() }}) as vocab_axis_cd
    , cast(primary_vterm_ind as {{ dbt.type_int() }}) as primary_vterm_ind
    , cast(source_string_keycap as {{ dbt.type_string() }}) as source_string_keycap
    , cast(cmti as {{ dbt.type_string() }}) as cmti
    , cast(primary_cterm_ind as {{ dbt.type_int() }}) as primary_cterm_ind
    , cast(concept_cki as {{ dbt.type_string() }}) as concept_cki
    , cast(disallowed_ind as {{ dbt.type_int() }}) as disallowed_ind
    , cast(source_identifier_keycap as {{ dbt.type_string() }}) as source_identifier_keycap
from {{ source('cerner_raw', 'nomenclature') }}
where active_ind = 1
and end_effective_dt_tm > current_timestamp()
