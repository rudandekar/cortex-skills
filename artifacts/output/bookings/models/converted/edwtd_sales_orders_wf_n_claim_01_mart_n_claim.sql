{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_claim', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CLAIM',
        'target_table': 'N_CLAIM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.192356+00:00'
    }
) }}

WITH 

source_w_claim AS (
    SELECT
        claim_key,
        sk_claim_id,
        claim_comments_txt,
        claim_reason_txt,
        sales_terr_asgmt_type_cd,
        claim_status_name,
        create_dtm,
        last_update_dtm,
        claim_usd_amt,
        claim_reason_descr,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_claim') }}
),

final AS (
    SELECT
        claim_key,
        sk_claim_id,
        claim_comments_txt,
        claim_reason_txt,
        sales_terr_asgmt_type_cd,
        claim_status_name,
        create_dtm,
        last_update_dtm,
        claim_usd_amt,
        claim_reason_descr,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_claim
)

SELECT * FROM final