{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_rev_cost_msr_tss_cogs_central', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_REV_COST_MSR_TSS_COGS_CENTRAL',
        'target_table': 'WI_PNL_TSS_COGS_N_TRNGLTD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.190537+00:00'
    }
) }}

WITH 

source_wi_pnl_tss_cogs_n_trngltd AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_product_key,
        dv_triangulation_type_id_int,
        bk_sales_territory_key,
        original_sales_territory_key,
        bk_trngltd_sales_territory_key,
        comp_us_net_rev_amt,
        dv_pnl_line_item_name,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_pnl_tss_cogs_n_trngltd') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_product_key,
        dv_triangulation_type_id_int,
        bk_sales_territory_key,
        original_sales_territory_key,
        bk_trngltd_sales_territory_key,
        comp_us_net_rev_amt,
        dv_pnl_line_item_name,
        dv_recurring_offer_cd
    FROM source_wi_pnl_tss_cogs_n_trngltd
)

SELECT * FROM final