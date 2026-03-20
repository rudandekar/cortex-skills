{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rev_sca_nrt_snap ', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_REV_SCA_NRT_SNAP ',
        'target_table': 'WI_REV_SCA_NRT_SNAP_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.671909+00:00'
    }
) }}

WITH 

source_el_xaas_trx_from_erp_sc AS (
    SELECT
        ar_trx_line_key,
        ar_trx_key,
        bk_batch_source_name,
        interface_line_attribute10,
        non_ref_trx_flag,
        is_exists_in_xaas_sc_flag,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'el_xaas_trx_from_erp_sc') }}
),

source_wi_rev_sca_nrt_snap AS (
    SELECT
        sales_credit_assignment_key,
        ep_source_line_id_int,
        sca_transaction_type,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sk_line_seq_id_int,
        ss_cd,
        sca_source_type_cd,
        sca_sales_commission_pct,
        bk_sales_credit_type_code,
        ep_sk_sales_credit_type_id_int,
        sales_rep_number,
        ep_sk_salesrep_id_int,
        sales_territory_key,
        ep_sk_territory_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_ar_transaction_line_key,
        ru_sales_order_line_key,
        start_tv_dtm,
        end_tv_dtm
    FROM {{ source('raw', 'wi_rev_sca_nrt_snap') }}
),

final AS (
    SELECT
        sales_credit_assignment_key,
        ep_source_line_id_int,
        sca_transaction_type,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sk_line_seq_id_int,
        ss_cd,
        sca_source_type_cd,
        sca_sales_commission_pct,
        bk_sales_credit_type_code,
        ep_sk_sales_credit_type_id_int,
        sales_rep_number,
        ep_sk_salesrep_id_int,
        sales_territory_key,
        ep_sk_territory_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_ar_transaction_line_key,
        ru_sales_order_line_key,
        start_tv_dtm,
        end_tv_dtm,
        originated_qtc_via_cg1_flg,
        sk_sc_agent_id_int
    FROM source_wi_rev_sca_nrt_snap
)

SELECT * FROM final