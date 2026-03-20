{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_apsp_gprv_fa_super_cat_asgn', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_APSP_GPRV_FA_SUPER_CAT_ASGN',
        'target_table': 'N_APSP_GPRV_FA_SUPER_CAT_ASGN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.824756+00:00'
    }
) }}

WITH 

source_w_apsp_gprv_fa_super_cat_asgn AS (
    SELECT
        bk_super_category_cd,
        bk_financial_account_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_gprv_fa_super_cat_asgn') }}
),

final AS (
    SELECT
        bk_super_category_cd,
        bk_financial_account_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_apsp_gprv_fa_super_cat_asgn
)

SELECT * FROM final