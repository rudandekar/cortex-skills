{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_srvc_contract_ch_flg', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_SRVC_CONTRACT_CH_FLG',
        'target_table': 'WI_CCM_SRVC_CONTRACT_CH_FLG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.942976+00:00'
    }
) }}

WITH 

source_wi_ccm_srvc_contract_ch_flg AS (
    SELECT
        src_rptd_service_contarct_num,
        channel_bookings_flg
    FROM {{ source('raw', 'wi_ccm_srvc_contract_ch_flg') }}
),

source_wi_ccm_srvc_contract AS (
    SELECT
        src_rptd_service_contarct_num
    FROM {{ source('raw', 'wi_ccm_srvc_contract') }}
),

final AS (
    SELECT
        src_rptd_service_contarct_num,
        channel_bookings_flg
    FROM source_wi_ccm_srvc_contract
)

SELECT * FROM final