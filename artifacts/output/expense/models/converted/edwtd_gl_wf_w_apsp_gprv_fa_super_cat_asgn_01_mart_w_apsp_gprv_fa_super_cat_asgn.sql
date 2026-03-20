{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_apsp_gprv_fa_super_cat_asgn', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_APSP_GPRV_FA_SUPER_CAT_ASGN',
        'target_table': 'W_APSP_GPRV_FA_SUPER_CAT_ASGN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.067325+00:00'
    }
) }}

WITH 

source_st_rv_account_hierarchy AS (
    SELECT
        hierarchy_type,
        category_id,
        category,
        category_level1,
        category_level2,
        account_number,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_rv_account_hierarchy') }}
),

final AS (
    SELECT
        bk_super_category_cd,
        bk_financial_account_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_rv_account_hierarchy
)

SELECT * FROM final