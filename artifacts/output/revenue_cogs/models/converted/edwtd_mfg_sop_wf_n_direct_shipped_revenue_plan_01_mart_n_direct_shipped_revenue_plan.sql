{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_direct_shipped_revenue_plan', 'batch', 'edwtd_mfg_sop'],
    meta={
        'source_workflow': 'wf_m_N_DIRECT_SHIPPED_REVENUE_PLAN',
        'target_table': 'N_DIRECT_SHIPPED_REVENUE_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.098259+00:00'
    }
) }}

WITH 

source_w_direct_shipped_revenue_plan AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_start_dt,
        bk_src_publish_dtm,
        dv_src_publish_dt,
        drct_shppd_rev_pln_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_direct_shipped_revenue_plan') }}
),

final AS (
    SELECT
        bk_product_family_id,
        bk_fiscal_week_start_dt,
        bk_src_publish_dtm,
        dv_src_publish_dt,
        drct_shppd_rev_pln_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_direct_shipped_revenue_plan
)

SELECT * FROM final