{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_cogs_proj_sol_aot_split', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_AS_COGS_PROJ_SOL_AOT_SPLIT',
        'target_table': 'WI_AS_COGS_PROJ_SOL_LNK_SPLIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.725377+00:00'
    }
) }}

WITH 

source_wi_as_cogs_proj_sol_final AS (
    SELECT
        bk_as_project_cd,
        sales_order_line_key,
        sol_identifier
    FROM {{ source('raw', 'wi_as_cogs_proj_sol_final') }}
),

source_wi_as_cogs_proj_sol_lnk_split AS (
    SELECT
        bk_as_project_cd,
        sales_order_line_key,
        psol_split_pct
    FROM {{ source('raw', 'wi_as_cogs_proj_sol_lnk_split') }}
),

source_wi_as_cogs_proj_sol_aot_split AS (
    SELECT
        bk_as_project_cd,
        dv_sales_order_line_key,
        bk_allocated_servc_group_id,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_goods_adj_prd_key,
        dv_cx_product,
        split_pct
    FROM {{ source('raw', 'wi_as_cogs_proj_sol_aot_split') }}
),

source_wi_as_cogs_bkgs_sol_aot AS (
    SELECT
        dv_sales_order_line_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_allocated_servc_group_id,
        bk_as_ato_tech_name,
        dv_goods_adj_prd_key,
        dv_cx_product,
        bookings_amount
    FROM {{ source('raw', 'wi_as_cogs_bkgs_sol_aot') }}
),

source_wi_table_name_param_pnl_as AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param_pnl_as') }}
),

final AS (
    SELECT
        bk_as_project_cd,
        sales_order_line_key,
        psol_split_pct
    FROM source_wi_table_name_param_pnl_as
)

SELECT * FROM final