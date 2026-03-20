{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_claim_end_cust', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CLAIM_END_CUST',
        'target_table': 'N_CLAIM_END_CUST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.417969+00:00'
    }
) }}

WITH 

source_w_claim_end_cust AS (
    SELECT
        claim_end_cust_key,
        sk_src_sav_detail_id,
        claim_key,
        cust_src_rptd_prty_key,
        sls_acct_grp_prty_key,
        cust_validated_prty_key,
        src_sav_dtl_seq_id_int,
        cust_cntry_name,
        create_dtm,
        last_update_dtm,
        split_pct,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_claim_end_cust') }}
),

final AS (
    SELECT
        claim_end_cust_key,
        sk_src_sav_detail_id,
        claim_key,
        cust_src_rptd_prty_key,
        sls_acct_grp_prty_key,
        cust_validated_prty_key,
        src_sav_dtl_seq_id_int,
        cust_cntry_name,
        create_dtm,
        last_update_dtm,
        split_pct,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_claim_end_cust
)

SELECT * FROM final