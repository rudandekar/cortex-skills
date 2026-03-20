{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_eac_rule_lookup', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_W_EAC_RULE_LOOKUP',
        'target_table': 'W_EAC_RULE_ATTRIB_LOOKUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.070889+00:00'
    }
) }}

WITH 

source_st_xxfsam_lookup_v AS (
    SELECT
        lookup_id,
        lookup_type,
        lookup_description,
        lookup_value,
        org_id,
        start_date,
        end_date,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by
    FROM {{ source('raw', 'st_xxfsam_lookup_v') }}
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
        edw_update_datetime,
        action_code,
        dml_type
    FROM source_st_xxfsam_lookup_v
)

SELECT * FROM final