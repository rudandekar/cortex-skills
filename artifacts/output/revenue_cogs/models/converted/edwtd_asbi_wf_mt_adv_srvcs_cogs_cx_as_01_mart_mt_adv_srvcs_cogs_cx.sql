{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_adv_srvcs_cogs_cx_as', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_MT_ADV_SRVCS_COGS_CX_AS',
        'target_table': 'MT_ADV_SRVCS_COGS_CX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.751265+00:00'
    }
) }}

WITH 

source_mt_adv_srvcs_cogs_cx AS (
    SELECT
        fiscal_year_month_int,
        bk_as_project_cd,
        sales_territory_key,
        goods_product_key,
        dv_cx_product,
        dv_cost_type,
        pnl_line_item_name,
        dv_match_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_financial_account_cd,
        dv_comp_us_net_cogs_usd_amt,
        dv_dept_number,
        bk_allocated_servc_group_id
    FROM {{ source('raw', 'mt_adv_srvcs_cogs_cx') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        bk_as_project_cd,
        sales_territory_key,
        goods_product_key,
        dv_cx_product,
        dv_cost_type,
        pnl_line_item_name,
        dv_match_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_financial_account_cd,
        dv_comp_us_net_cogs_usd_amt,
        dv_dept_number,
        bk_allocated_servc_group_id
    FROM source_mt_adv_srvcs_cogs_cx
)

SELECT * FROM final