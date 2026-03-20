{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_disti_claim_pos_trans', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_DISTI_CLAIM_POS_TRANS',
        'target_table': 'WI_DISTI_CLAIM_POS_TRANS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.162810+00:00'
    }
) }}

WITH 

source_wi_disti_claim_pos_trans AS (
    SELECT
        bk_pos_transaction_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'wi_disti_claim_pos_trans') }}
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_dtm,
        edw_update_user
    FROM source_wi_disti_claim_pos_trans
)

SELECT * FROM final