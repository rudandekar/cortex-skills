{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_apsp_gprv_fin_acct_super_cat', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_APSP_GPRV_FIN_ACCT_SUPER_CAT',
        'target_table': 'W_APSP_GPRV_FIN_ACCT_SUPER_CAT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.787577+00:00'
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_rv_account_hierarchy
)

SELECT * FROM final