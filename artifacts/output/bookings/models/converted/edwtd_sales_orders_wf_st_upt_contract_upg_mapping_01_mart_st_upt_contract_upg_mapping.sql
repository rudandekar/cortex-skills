{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_upt_contract_upg_mapping', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_UPT_CONTRACT_UPG_MAPPING',
        'target_table': 'ST_UPT_CONTRACT_UPG_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.419198+00:00'
    }
) }}

WITH 

source_ff_upt_contract_upg_mapping AS (
    SELECT
        upgrade_product_number,
        upgrade_product_mapping,
        upgrade_product_family,
        updated_date
    FROM {{ source('raw', 'ff_upt_contract_upg_mapping') }}
),

final AS (
    SELECT
        upgrade_product_number,
        upgrade_product_mapping,
        upgrade_product_family,
        updated_date
    FROM source_ff_upt_contract_upg_mapping
)

SELECT * FROM final