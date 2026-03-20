{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_acqstn_cntry_enblmt_rev_pln', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_W_ACQSTN_CNTRY_ENBLMT_REV_PLN',
        'target_table': 'W_ACQSTN_CNTRY_ENBLMT_REV_PLN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.285881+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_crp_total_shpd_rev AS (
    SELECT
        product_family,
        fiscal_week_start_date,
        direct_ship_revenue,
        acquisition_ce,
        creation_date
    FROM {{ source('raw', 'st_xxcmf_sp_crp_total_shpd_rev') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxcmf_sp_crp_total_shpd_rev
)

SELECT * FROM final