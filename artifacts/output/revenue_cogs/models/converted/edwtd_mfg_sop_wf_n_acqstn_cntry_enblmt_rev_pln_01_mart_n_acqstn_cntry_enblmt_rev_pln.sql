{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_acqstn_cntry_enblmt_rev_pln', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_N_ACQSTN_CNTRY_ENBLMT_REV_PLN',
        'target_table': 'N_ACQSTN_CNTRY_ENBLMT_REV_PLN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.328162+00:00'
    }
) }}

WITH 

source_w_acqstn_cntry_enblmt_rev_pln AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_start_dt,
        bk_src_publish_dtm,
        dv_src_publish_dt,
        acquisition_plan_rev_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_acqstn_cntry_enblmt_rev_pln') }}
),

final AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_start_dt,
        bk_src_publish_dtm,
        dv_src_publish_dt,
        acquisition_plan_rev_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_acqstn_cntry_enblmt_rev_pln
)

SELECT * FROM final