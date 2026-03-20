{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_prjct_to_coo_lk', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_PRJCT_TO_COO_LK',
        'target_table': 'N_PRJCT_TO_COO_LK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.390583+00:00'
    }
) }}

WITH 

source_w_prjct_to_coo_lk AS (
    SELECT
        bk_iso_country_cd,
        bk_coo_prjct_name,
        src_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_prjct_to_coo_lk') }}
),

final AS (
    SELECT
        bk_iso_country_cd,
        bk_coo_prjct_name,
        src_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_prjct_to_coo_lk
)

SELECT * FROM final