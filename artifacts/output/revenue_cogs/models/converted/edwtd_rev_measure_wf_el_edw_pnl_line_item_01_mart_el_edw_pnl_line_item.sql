{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_edw_pnl_line_item', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_EDW_PNL_LINE_ITEM',
        'target_table': 'EL_EDW_PNL_LINE_ITEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.523984+00:00'
    }
) }}

WITH 

source_el_edw_pnl_line_item AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        revenue_measure_key,
        rev_measure_trans_type_cd,
        dv_pnl_line_item_name,
        allocation_percentage,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_edw_pnl_line_item') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        revenue_measure_key,
        rev_measure_trans_type_cd,
        dv_pnl_line_item_name,
        allocation_percentage,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_el_edw_pnl_line_item
)

SELECT * FROM final