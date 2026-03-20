{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_line_service_revenue_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_LINE_SERVICE_REVENUE_AS',
        'target_table': 'WI_PNL_LINE_ITEM_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.986263+00:00'
    }
) }}

WITH 

source_wi_pnl_line_item_as AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        revenue_measure_key,
        dv_pnl_line_item_name
    FROM {{ source('raw', 'wi_pnl_line_item_as') }}
),

source_wi_service_revenue_as AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        revenue_measure_key,
        rev_measure_trans_type_cd,
        sales_territory_key,
        service_product_key,
        goods_product_key,
        bk_allocated_servc_group_id,
        bk_service_category_id,
        data_source_name,
        data_set_cd,
        comp_us_net_rev_amt,
        dv_recurring_offer_cd,
        bk_busi_svc_offer_type_name,
        dv_gsp_or_cx_product
    FROM {{ source('raw', 'wi_service_revenue_as') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        revenue_measure_key,
        dv_pnl_line_item_name
    FROM source_wi_service_revenue_as
)

SELECT * FROM final