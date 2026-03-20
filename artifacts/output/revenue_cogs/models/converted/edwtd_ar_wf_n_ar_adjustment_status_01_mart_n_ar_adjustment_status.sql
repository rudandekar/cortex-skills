{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_adjustment_status', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_ADJUSTMENT_STATUS',
        'target_table': 'N_AR_ADJUSTMENT_STATUS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.553272+00:00'
    }
) }}

WITH 

source_w_ar_adjustment_status AS (
    SELECT
        bk_ar_adjustment_status_cd,
        ar_adjustment_status_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_adjustment_status') }}
),

final AS (
    SELECT
        bk_ar_adjustment_status_cd,
        ar_adjustment_status_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ar_adjustment_status
)

SELECT * FROM final