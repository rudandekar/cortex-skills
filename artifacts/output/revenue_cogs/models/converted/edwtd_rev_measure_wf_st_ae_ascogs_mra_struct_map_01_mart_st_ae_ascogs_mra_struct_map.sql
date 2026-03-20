{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_ascogs_mra_struct_map', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_AE_ASCOGS_MRA_STRUCT_MAP',
        'target_table': 'ST_AE_ASCOGS_MRA_STRUCT_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.752749+00:00'
    }
) }}

WITH 

source_ff_ae_ascogs_mra_struct_map AS (
    SELECT
        fiscal_month_id,
        pnl_measure_name,
        pl_node_value,
        pl_node_level,
        gl_account_code,
        account_include_flag,
        allocation_method,
        ges_update_date
    FROM {{ source('raw', 'ff_ae_ascogs_mra_struct_map') }}
),

final AS (
    SELECT
        fiscal_month_id,
        pnl_measure_name,
        pl_node_value,
        pl_node_level,
        gl_account_code,
        account_include_flag,
        allocation_method,
        ges_update_date
    FROM source_ff_ae_ascogs_mra_struct_map
)

SELECT * FROM final