{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drr_sales_acc_msr_wwdist', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRR_SALES_ACC_MSR_WWDIST',
        'target_table': 'WI_DRR_SALES_ACNT_MEASURE_FNL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.938853+00:00'
    }
) }}

WITH 

source_wi_drr_bookings_party_enrich AS (
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
        dv_end_cust_party_key,
        reason_desc,
        fin_adj_key,
        process_type,
        rule_process_flag
    FROM {{ source('raw', 'wi_drr_bookings_party_enrich') }}
),

source_mt_namd_acnt_bkgs_measure AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bk_sa_member_id_int,
        bkgs_measure_trans_type_code,
        assignment_source_system_cd,
        end_cust_ownership_split_pct,
        enrichment_customer_type_cd,
        field_vldtd_end_cust_prty_key,
        hq_remote_cd,
        last_updated_by_user_name,
        last_updated_dtm,
        no_vld_sls_acct_pty_rsn_name,
        owned_flg,
        sales_account_group_party_key,
        sk_trx_party_extension_id_int,
        sls_acct_grp_member_party_key,
        validation_mode_cd,
        validation_status_cd,
        dv_end_cust_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_fmv_flg
    FROM {{ source('raw', 'mt_namd_acnt_bkgs_measure') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        dv_end_cust_party_key,
        new_sa_member_id_int,
        link_customer_party_key,
        sales_account_group_party_key,
        reason_desc_end_cust,
        reason_descr,
        sales_credit_split_pct,
        sls_acct_group_sales_terr_key,
        sales_rep_num,
        ep_otm_territory_id_int,
        fin_adj_key,
        process_type,
        node_enrich_flag,
        node_enrich_reason
    FROM source_mt_namd_acnt_bkgs_measure
)

SELECT * FROM final