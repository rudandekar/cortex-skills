{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_crp_net_bookings', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_CRP_NET_BOOKINGS',
        'target_table': 'ST_XXCMF_SP_CRP_NET_BOOKINGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.807565+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_crp_net_bookings AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        sales_channel,
        net_bookings_total,
        creation_date
    FROM {{ source('raw', 'ff_xxcmf_sp_crp_net_bookings') }}
),

final AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        sales_channel,
        net_bookings_total,
        creation_date
    FROM source_ff_xxcmf_sp_crp_net_bookings
)

SELECT * FROM final