{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_mta_cogs', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_MTA_COGS',
        'target_table': 'WI_MTA_COGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.865600+00:00'
    }
) }}

WITH 

source_n_material_transaction AS (
    SELECT
        material_transaction_key,
        transaction_reference_descr,
        ru_sales_order_key,
        ru_purchase_order_key,
        ru_sales_order_line_key,
        source_type,
        actual_cost_usd_amt,
        transaction_dtm,
        transaction_qty,
        sk_transaction_id_int,
        ss_cd,
        inventory_organization_key,
        item_key,
        bk_transaction_type_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_material_transaction') }}
),

final AS (
    SELECT
        sales_order_key,
        sales_order_line_key,
        ru_parnt_sls_order_line_key,
        bk_so_number_int,
        sk_so_line_id_int,
        sk_inventory_item_id_int,
        ss_code,
        item_type_code,
        dv_fiscal_year_mth_number_int,
        transaction_dtm,
        mta_base_transaction_value,
        dv_comp_us_mta_cost_amt
    FROM source_n_material_transaction
)

SELECT * FROM final