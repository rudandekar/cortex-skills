{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_ap_crt_card_trxns_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_AP_CRT_CARD_TRXNS_ALL',
        'target_table': 'ST_MF_AP_CRT_CARD_TRXNS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.928573+00:00'
    }
) }}

WITH 

source_ff_mf_ap_crt_card_trxns_all AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
        trx_id,
        sic_code,
        mis_industry_code,
        last_update_date
    FROM {{ source('raw', 'ff_mf_ap_crt_card_trxns_all') }}
),

final AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
        trx_id,
        sic_code,
        mis_industry_code,
        last_update_date
    FROM source_ff_mf_ap_crt_card_trxns_all
)

SELECT * FROM final