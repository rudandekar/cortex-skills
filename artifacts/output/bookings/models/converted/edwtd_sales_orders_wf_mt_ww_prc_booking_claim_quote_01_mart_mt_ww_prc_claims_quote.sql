{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_ww_prc_claims_quote', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_WW_PRC_CLAIMS_QUOTE',
        'target_table': 'MT_WW_PRC_CLAIMS_QUOTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.870979+00:00'
    }
) }}

WITH 

source_mt_ww_prc_claims_quote AS (
    SELECT
        ru_bk_pos_transaction_id_int,
        bk_deal_id,
        product_key,
        dv_promotion_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        adjustment_type_cd,
        dv_source_cd,
        dv_program_cd,
        dv_deal_promotion_cd,
        dv_deal_program_cd,
        dv_seq_int
    FROM {{ source('raw', 'mt_ww_prc_claims_quote') }}
),

source_mt_ww_prc_booking AS (
    SELECT
        sales_territory_key,
        adjustment_type_cd,
        service_flg,
        bookings_process_dt,
        dv_posted_dt,
        dv_deal_cd,
        dv_disti_reported_deal_cd,
        dv_so_number_int,
        product_key,
        partner_site_party_key,
        dv_distributor_id_int,
        dv_dsv_pos_factor_int,
        dv_pos_trx_id_int,
        distributor_reported_deal_id,
        dv_claim_auth_name,
        channel_bookings_flg,
        bill_to_customer_key,
        sold_to_customer_key,
        sales_account_group_party_key,
        field_vldtd_end_cust_prty_key,
        sls_acct_grp_member_party_key,
        sales_rep_num,
        end_customer_key,
        adjustment_cd,
        disti_country_cd,
        bk_product_id,
        dv_book_cost_amt,
        dv_book_qty,
        dv_book_net_amt,
        dv_book_list_amt,
        dv_concession_line_lvl_name,
        dv_concession_deal_lvl_name,
        dv_program_line_lvl_name,
        dv_program_deal_lvl_name,
        dv_concession_type_name,
        dv_runrate_flg,
        dv_fasttrack_name,
        dv_fasttrack_as_was_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        fiscal_year_quarter_number_int
    FROM {{ source('raw', 'mt_ww_prc_booking') }}
),

final AS (
    SELECT
        ru_bk_pos_transaction_id_int,
        bk_deal_id,
        product_key,
        dv_promotion_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        adjustment_type_cd,
        dv_source_cd,
        dv_program_cd,
        dv_deal_promotion_cd,
        dv_deal_program_cd,
        dv_seq_int
    FROM source_mt_ww_prc_booking
)

SELECT * FROM final