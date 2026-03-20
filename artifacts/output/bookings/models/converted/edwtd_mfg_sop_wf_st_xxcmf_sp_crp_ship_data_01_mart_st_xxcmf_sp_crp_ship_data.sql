{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_crp_ship_data', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_CRP_SHIP_DATA',
        'target_table': 'ST_XXCMF_SP_CRP_SHIP_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.978939+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_crp_ship_data AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        sales_channel,
        delivery_type,
        ship_plan,
        creation_date
    FROM {{ source('raw', 'ff_xxcmf_sp_crp_ship_data') }}
),

final AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        sales_channel,
        delivery_type,
        ship_plan,
        creation_date
    FROM source_ff_xxcmf_sp_crp_ship_data
)

SELECT * FROM final