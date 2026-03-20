{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drr_bookings_incrmental', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRR_BOOKINGS_INCRMENTAL',
        'target_table': 'WI_FIN_ADJ_BOOKINGS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.802070+00:00'
    }
) }}

WITH 

source_wi_fin_adj_bookings_active AS (
    SELECT
        dyn_restmt_sls_adjstmt_key,
        original_sales_terr_key,
        restated_sales_terr_key,
        sales_order_line_key,
        bk_bookings_trx_type_cd,
        bk_allocation_split_pct,
        bk_effective_dt,
        sk_trx_id_bigint,
        restatement_sub_type_cd,
        cust_prty_key,
        do_not_restate_flg,
        expiration_dt,
        apprvd_by_csco_wrkr_prty_key,
        uploaded_by_csco_wrkr_prty_key,
        approved_dtm,
        dv_approved_dt,
        uploaded_dtm,
        dv_uploaded_dt,
        validation_status_cd,
        validation_status_reason_descr,
        upload_file_name,
        processed_flg,
        ar_trx_line_key,
        bk_pos_trx_id_int,
        bk_sales_adj_line_num_int,
        so_subscr_item_sales_trx_key,
        rev_transfer_key,
        sales_acct_grp_prty_key,
        dv_bookings_trx_ref_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'wi_fin_adj_bookings_active') }}
),

source_wi_drr_bookings_measure_incr AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        sales_territory_key,
        sales_rep_number,
        end_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_deal_id,
        wips_originator_id_int,
        edw_update_datetime,
        bill_to_customer_key
    FROM {{ source('raw', 'wi_drr_bookings_measure_incr') }}
),

source_wi_drr_bookings_incrmental AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        sales_territory_key,
        sales_rep_number,
        end_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_deal_id,
        wips_originator_id_int,
        l1_sales_territory_name_code,
        l5_sales_territory_name_code,
        l6_sales_territory_name_code,
        iso_country_code,
        field_vldtd_end_cust_prty_key,
        sales_coverage_code,
        sales_territory_type_code,
        sales_account_group_party_key,
        no_vld_sls_acct_pty_rsn_name,
        fin_adj_key,
        process_type,
        end_customer_process_flag,
        edw_update_datetime,
        bill_to_customer_key,
        party_reason_descr
    FROM {{ source('raw', 'wi_drr_bookings_incrmental') }}
),

source_wi_drr_mt_sls_acnt_rstd_incr AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        dv_end_cust_party_key,
        sales_account_group_party_key
    FROM {{ source('raw', 'wi_drr_mt_sls_acnt_rstd_incr') }}
),

source_wi_fin_adj_bookings_all AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        sales_territory_key,
        sales_rep_number,
        end_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_deal_id,
        wips_originator_id_int,
        l1_sales_territory_name_code,
        l5_sales_territory_name_code,
        l6_sales_territory_name_code,
        iso_country_code,
        cust_prty_key,
        sales_coverage_code,
        sales_territory_type_code,
        bk_sales_acct_id_int,
        bk_allocation_split_pct,
        restatement_sub_type_cd,
        process_type,
        end_customer_process_flag,
        bill_to_customer_key
    FROM {{ source('raw', 'wi_fin_adj_bookings_all') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        sales_territory_key,
        sales_rep_number,
        end_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_deal_id,
        wips_originator_id_int,
        l1_sales_territory_name_code,
        l5_sales_territory_name_code,
        l6_sales_territory_name_code,
        iso_country_code,
        cust_prty_key,
        sales_coverage_code,
        sales_territory_type_code,
        sales_acct_grp_prty_key,
        bk_allocation_split_pct,
        restatement_sub_type_cd,
        fin_adj_key,
        process_type,
        end_customer_process_flag,
        bill_to_customer_key
    FROM source_wi_fin_adj_bookings_all
)

SELECT * FROM final