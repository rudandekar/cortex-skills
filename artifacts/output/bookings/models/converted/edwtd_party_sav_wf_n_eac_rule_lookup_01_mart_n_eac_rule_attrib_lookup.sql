{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_rule_lookup', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_RULE_LOOKUP',
        'target_table': 'N_EAC_RULE_ATTRIB_LOOKUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.615022+00:00'
    }
) }}

WITH 

source_w_eac_rule_attrib_lookup AS (
    SELECT
        eac_rule_attrib_lookup_id,
        bk_eac_org_id,
        attribute_name,
        attribute_value_name,
        attribute_value_text,
        effective_end_dt,
        effective_start_dt,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_rule_attrib_lookup') }}
),

final AS (
    SELECT
        eac_rule_attrib_lookup_id,
        bk_eac_org_id,
        attribute_name,
        attribute_value_name,
        attribute_value_text,
        effective_end_dt,
        effective_start_dt,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_w_eac_rule_attrib_lookup
)

SELECT * FROM final