select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
      cast(null as {{ dbt.type_string() }}) as claim_id
    , cast(null as {{ dbt.type_string() }}) as claim_line_number
    , cast(null as {{ dbt.type_string() }}) as person_id
    , cast(null as {{ dbt.type_string() }}) as member_id
    , cast(null as {{ dbt.type_string() }}) as payer
    , cast(null as {{ dbt.type_string() }}) as plan
    , cast(null as {{ dbt.type_string() }}) as prescribing_provider_npi
    , cast(null as {{ dbt.type_string() }}) as dispensing_provider_npi
    , cast(null as {{ dbt.type_string() }}) as dispensing_date
    , cast(null as {{ dbt.type_string() }}) as ndc_code
    , cast(null as {{ dbt.type_string() }}) as quantity
    , cast(null as {{ dbt.type_string() }}) as days_supply
    , cast(null as {{ dbt.type_string() }}) as refills
    , cast(null as {{ dbt.type_string() }}) as paid_date
    , cast(null as {{ dbt.type_string() }}) as paid_amount
    , cast(null as {{ dbt.type_string() }}) as allowed_amount
    , cast(null as {{ dbt.type_string() }}) as charge_amount
    , cast(null as {{ dbt.type_string() }}) as coinsurance_amount
    , cast(null as {{ dbt.type_string() }}) as copayment_amount
    , cast(null as {{ dbt.type_string() }}) as deductible_amount
    , cast(null as {{ dbt.type_string() }}) as in_network_flag
    , cast(null as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_string() }}) as file_date
    , cast(null as {{ dbt.type_string() }}) as ingest_datetime
{% if target.type == 'fabric' %} {% else %} limit 0 {% endif %}
