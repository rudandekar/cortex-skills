{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_hts_classification_rule', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_HTS_CLASSIFICATION_RULE',
        'target_table': 'SM_HTS_CLASSIFICATION_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.228361+00:00'
    }
) }}

WITH 

source_w_hts_classification_rule AS (
    SELECT
        hts_classification_rule_key,
        rule_override_flg,
        sk_rule_id_int,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        customs_item_specific_key,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_hts_classification_rule') }}
),

source_ex_xxcfi_cb_rules AS (
    SELECT
        batch_id,
        rule_id,
        specific_id,
        specific_name,
        override_key_in,
        start_date,
        end_date,
        override_flag,
        active_flag,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code,
        exception_type,
        audit_flag
    FROM {{ source('raw', 'ex_xxcfi_cb_rules') }}
),

source_sm_hts_classification_rule AS (
    SELECT
        hts_classification_rule_key,
        sk_rule_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_hts_classification_rule') }}
),

final AS (
    SELECT
        hts_classification_rule_key,
        sk_rule_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_hts_classification_rule
)

SELECT * FROM final