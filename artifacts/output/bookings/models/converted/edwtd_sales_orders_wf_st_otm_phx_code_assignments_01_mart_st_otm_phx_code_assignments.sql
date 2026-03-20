{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_phx_code_assignments', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_PHX_CODE_ASSIGNMENTS',
        'target_table': 'ST_OTM_PHX_CODE_ASSIGNMENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.728020+00:00'
    }
) }}

WITH 

source_st_otm_phx_code_assignments AS (
    SELECT
        last_update_date,
        rebok_channel_flag,
        territory_type_code,
        trx_sc_id
    FROM {{ source('raw', 'st_otm_phx_code_assignments') }}
),

final AS (
    SELECT
        last_update_date,
        rebok_channel_flag,
        territory_type_code,
        trx_sc_id
    FROM source_st_otm_phx_code_assignments
)

SELECT * FROM final