{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cis_service_cogs_allocation', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_CIS_SERVICE_COGS_ALLOCATION',
        'target_table': 'N_CIS_SERVICE_COGS_ALLOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.115742+00:00'
    }
) }}

WITH 

source_wi_ro_keys_srvc_cogs_alloctn AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_ro_keys_srvc_cogs_alloctn') }}
),

source_n_cis_service_cogs_allocation AS (
    SELECT
        bk_fiscal_month_num_int,
        bk_dept_cd,
        bk_company_cd,
        sales_territory_key,
        product_key,
        bk_pnl_line_item_name,
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        rev_or_cogs_type_cd,
        cis_cogs_allocation_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_cis_service_cogs_allocation') }}
),

final AS (
    SELECT
        bk_fiscal_month_num_int,
        bk_dept_cd,
        bk_company_cd,
        sales_territory_key,
        product_key,
        bk_pnl_line_item_name,
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        rev_or_cogs_type_cd,
        cis_cogs_allocation_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_cis_service_cogs_allocation
)

SELECT * FROM final