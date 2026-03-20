{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_shipping_milestone_link', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SHIPPING_MILESTONE_LINK',
        'target_table': 'N_SHIPPING_MILESTONE_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.058453+00:00'
    }
) }}

WITH 

source_w_shipping_milestone_link AS (
    SELECT
        smt_key,
        inter_event_drtn_seconds_cnt,
        ssmt_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_shipping_milestone_link') }}
),

final AS (
    SELECT
        smt_key,
        inter_event_drtn_seconds_cnt,
        ssmt_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_shipping_milestone_link
)

SELECT * FROM final