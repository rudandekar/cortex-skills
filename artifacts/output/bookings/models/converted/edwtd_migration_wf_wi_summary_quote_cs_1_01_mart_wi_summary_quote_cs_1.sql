{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_summary_quote_cs_1', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_WI_SUMMARY_QUOTE_CS_1',
        'target_table': 'WI_SUMMARY_QUOTE_CS_1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.542106+00:00'
    }
) }}

WITH 

source_wi_summary_quote_cs_1 AS (
    SELECT
        sol_bk_so_number_int,
        sol_sales_order_line_key,
        sol_sales_order_key,
        bk_service_quote_num,
        quote_summary_quote_flg,
        bkg_contract_number,
        bkgs_svc_lvl,
        summary_quote_case_num,
        ru_cisco_booked_datetime,
        fiscal_quarter_name,
        fiscal_year_number_int,
        item_categorization,
        attributed_product,
        sk_offer_attribution_id_int,
        as_ts_flg,
        as_architecture_name,
        technology_group_id,
        attr_prdt_offer_type_name
    FROM {{ source('raw', 'wi_summary_quote_cs_1') }}
),

final AS (
    SELECT
        sol_bk_so_number_int,
        sol_sales_order_line_key,
        sol_sales_order_key,
        bk_service_quote_num,
        quote_summary_quote_flg,
        bkg_contract_number,
        bkgs_svc_lvl,
        summary_quote_case_num,
        ru_cisco_booked_datetime,
        fiscal_quarter_name,
        fiscal_year_number_int,
        item_categorization,
        attributed_product,
        sk_offer_attribution_id_int,
        as_ts_flg,
        as_architecture_name,
        technology_group_id,
        attr_prdt_offer_type_name
    FROM source_wi_summary_quote_cs_1
)

SELECT * FROM final