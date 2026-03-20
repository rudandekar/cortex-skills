{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_edw_pnl_line_item', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_EDW_PNL_LINE_ITEM',
        'target_table': 'WI_EDW_PNL_LINE_ITEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.492984+00:00'
    }
) }}

WITH 

source_wi_edw_pnl_line_item AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        revenue_measure_key,
        rev_measure_trans_type_cd,
        allocation_percentage,
        pnl_line_item_name
    FROM {{ source('raw', 'wi_edw_pnl_line_item') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        revenue_measure_key,
        rev_measure_trans_type_cd,
        allocation_percentage,
        pnl_line_item_name
    FROM source_wi_edw_pnl_line_item
)

SELECT * FROM final