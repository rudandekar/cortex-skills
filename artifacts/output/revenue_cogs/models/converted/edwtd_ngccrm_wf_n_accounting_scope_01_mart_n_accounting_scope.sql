{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_accounting_scope', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_ACCOUNTING_SCOPE',
        'target_table': 'N_ACCOUNTING_SCOPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.865714+00:00'
    }
) }}

WITH 

source_w_accounting_scope AS (
    SELECT
        bk_accounting_scope_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_accounting_scope') }}
),

final AS (
    SELECT
        bk_accounting_scope_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_accounting_scope
)

SELECT * FROM final