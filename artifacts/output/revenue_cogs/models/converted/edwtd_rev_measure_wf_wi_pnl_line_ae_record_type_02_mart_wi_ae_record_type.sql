{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_line_ae_record_type', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_LINE_AE_RECORD_TYPE',
        'target_table': 'WI_AE_RECORD_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.708591+00:00'
    }
) }}

WITH 

source_wi_pnl_line_item AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        revenue_measure_key,
        dv_pnl_line_item_name,
        rev_split_pct
    FROM {{ source('raw', 'wi_pnl_line_item') }}
),

source_wi_ae_record_type AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_sales_territory_key,
        record_type
    FROM {{ source('raw', 'wi_ae_record_type') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_sales_territory_key,
        record_type
    FROM source_wi_ae_record_type
)

SELECT * FROM final