{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ncr_bkg_trx_retro', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_WI_NCR_BKG_TRX_RETRO',
        'target_table': 'WI_NCR_BKG_TRX_RETRO_SO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.207258+00:00'
    }
) }}

WITH 

source_wi_sca_retro_nrt_3 AS (
    SELECT
        sales_credit_assignment_key,
        ep_source_line_id_int,
        sca_source_commit_dtm,
        sca_source_commit_date,
        sca_source_update_dtm,
        ss_cd,
        global_name,
        sales_rep_number,
        new_sales_rep_number,
        ep_sk_salesrep_id_int,
        sales_territory_key,
        new_sales_territory_key,
        ep_sk_territory_id_int,
        ru_sales_order_line_key,
        new_ru_sales_order_line_key,
        bk_sales_credit_type_code,
        new_bk_sales_credit_type_code,
        ep_sk_sales_credit_type_id_int,
        sca_source_type_cd
    FROM {{ source('raw', 'wi_sca_retro_nrt_3') }}
),

source_wi_sol_nrt_retro_3 AS (
    SELECT
        sales_order_line_key,
        ep_header_id_int,
        sales_order_key,
        new_sales_order_key,
        ss_cd,
        product_key,
        new_product_key,
        ep_product_inv_item_id_int,
        ship_to_customer_key,
        new_ship_to_customer_key,
        ep_stc_ship_to_org_id_int,
        so_line_source_update_dtm,
        source_commit_dtm,
        source_commit_date
    FROM {{ source('raw', 'wi_sol_nrt_retro_3') }}
),

final AS (
    SELECT
        drvd_ncr_bkg_trx_key,
        sales_order_line_key,
        sales_order_key,
        sales_rep_number,
        sold_to_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        product_key,
        sales_territory_key,
        sales_credit_type_code,
        sales_credit_asgn_key,
        so_source_commit_dt,
        sol_source_commit_dt,
        sca_source_commit_dt
    FROM source_wi_sol_nrt_retro_3
)

SELECT * FROM final