{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_hts_classification_rule_tv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_HTS_CLASSIFICATION_RULE_TV',
        'target_table': 'N_HTS_CLASSIFICATION_RULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.720091+00:00'
    }
) }}

WITH 

source_n_hts_classification_rule_tv AS (
    SELECT
        hts_classification_rule_key,
        rule_override_flg,
        sk_rule_id_int,
        source_deleted_flg,
        bk_item_specific_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        customs_item_specific_key
    FROM {{ source('raw', 'n_hts_classification_rule_tv') }}
),

source_n_hts_classification_rule AS (
    SELECT
        hts_classification_rule_key,
        rule_override_flg,
        source_deleted_flg,
        bk_item_specific_name,
        sk_rule_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        customs_item_specific_key
    FROM {{ source('raw', 'n_hts_classification_rule') }}
),

final AS (
    SELECT
        hts_classification_rule_key,
        rule_override_flg,
        source_deleted_flg,
        sk_rule_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        customs_item_specific_key
    FROM source_n_hts_classification_rule
)

SELECT * FROM final