{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_claim_end_cust', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CLAIM_END_CUST',
        'target_table': 'W_CLAIM_END_CUST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.757749+00:00'
    }
) }}

WITH 

source_st_xxnce_claim_sav_det AS (
    SELECT
        sav_det_id,
        sav_det_seq,
        sav_id,
        sav_name,
        biz_party_id,
        biz_party_name,
        sav_split_pct,
        end_user_party_id,
        end_user_party_name,
        created_by,
        creation_date,
        last_updated_by,
        updated_date,
        object_version_number,
        claim_id,
        end_user_country
    FROM {{ source('raw', 'st_xxnce_claim_sav_det') }}
),

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

transformed_exp_st_xxnce_claim_sav_det AS (
    SELECT
    sav_det_id,
    sav_det_seq,
    sav_id,
    sav_name,
    biz_party_id,
    biz_party_name,
    sav_split_pct,
    end_user_party_id,
    end_user_party_name,
    created_by,
    creation_date,
    last_updated_by,
    updated_date,
    object_version_number,
    claim_id,
    end_user_country,
    exception_type
    FROM source_w_claim_end_cust
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
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exp_st_xxnce_claim_sav_det
)

SELECT * FROM final