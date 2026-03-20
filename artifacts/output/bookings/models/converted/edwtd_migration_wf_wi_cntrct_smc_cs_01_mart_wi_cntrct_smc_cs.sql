{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cntrct_smc_cs', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_WI_CNTRCT_SMC_CS',
        'target_table': 'WI_CNTRCT_SMC_CS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.546627+00:00'
    }
) }}

WITH 

source_wi_cntrct_smc_cs AS (
    SELECT
        sk_id_lint,
        architecture_id,
        technology_id,
        offer_type_id,
        deal_end_cr_party_id,
        deal_end_cr_hq_party_id,
        source_create_dtm,
        maintenance_order_num
    FROM {{ source('raw', 'wi_cntrct_smc_cs') }}
),

final AS (
    SELECT
        sk_id_lint,
        architecture_id,
        technology_id,
        offer_type_id,
        deal_end_cr_party_id,
        deal_end_cr_hq_party_id,
        source_create_dtm,
        maintenance_order_num
    FROM source_wi_cntrct_smc_cs
)

SELECT * FROM final