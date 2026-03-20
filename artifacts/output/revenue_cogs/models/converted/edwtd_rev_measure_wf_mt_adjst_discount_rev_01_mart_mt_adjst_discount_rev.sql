{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_adjst_discount_rev', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_ADJST_DISCOUNT_REV',
        'target_table': 'MT_ADJST_DISCOUNT_REV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.784203+00:00'
    }
) }}

WITH 

source_mt_adjst_discount_rev AS (
    SELECT
        revenue_measure_key,
        sales_territory_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        discount_category,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'mt_adjst_discount_rev') }}
),

final AS (
    SELECT
        revenue_measure_key,
        sales_territory_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        discount_category,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM source_mt_adjst_discount_rev
)

SELECT * FROM final