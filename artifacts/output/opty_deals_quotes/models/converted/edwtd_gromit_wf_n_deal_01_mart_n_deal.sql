{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal', 'batch', 'edwtd_gromit'],
    meta={
        'source_workflow': 'wf_m_N_DEAL',
        'target_table': 'N_DEAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.878218+00:00'
    }
) }}

WITH 

source_w_deal AS (
    SELECT
        bk_deal_id,
        bk_deal_status_cd,
        deal_created_dtm,
        opportunity_key,
        expected_deal_booking_usd_amt,
        ru_fn_cntlr_csco_wrkr_prty_key,
        ru_lgl_asrr_csco_wrkr_prty_key,
        ru_src_rptd_dl_svc_cst_usd_amt,
        ru_approved_dt,
        deal_expiration_dt,
        deal_type,
        approved_role,
        deal_last_update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal') }}
),

final AS (
    SELECT
        bk_deal_id,
        bk_deal_status_cd,
        deal_created_dtm,
        opportunity_key,
        expected_deal_booking_usd_amt,
        ru_fn_cntlr_csco_wrkr_prty_key,
        ru_lgl_asrr_csco_wrkr_prty_key,
        ru_src_rptd_dl_svc_cst_usd_amt,
        ru_approved_dt,
        deal_expiration_dt,
        deal_type,
        approved_role,
        deal_last_update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_deal
)

SELECT * FROM final