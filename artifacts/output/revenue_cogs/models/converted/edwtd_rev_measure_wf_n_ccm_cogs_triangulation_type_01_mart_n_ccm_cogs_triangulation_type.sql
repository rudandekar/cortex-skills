{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccm_cogs_triangulation_type', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_CCM_COGS_TRIANGULATION_TYPE',
        'target_table': 'N_CCM_COGS_TRIANGULATION_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.028393+00:00'
    }
) }}

WITH 

source_w_ccm_cogs_triangulation_type AS (
    SELECT
        bk_triangulation_type_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccm_cogs_triangulation_type') }}
),

final AS (
    SELECT
        bk_triangulation_type_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccm_cogs_triangulation_type
)

SELECT * FROM final