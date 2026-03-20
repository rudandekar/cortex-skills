{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fixed_asset_transaction_type', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIXED_ASSET_TRANSACTION_TYPE',
        'target_table': 'N_FIXED_ASSET_TRANSACTION_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.624761+00:00'
    }
) }}

WITH 

source_w_fixed_asset_transaction_type AS (
    SELECT
        bk_fa_transaction_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fixed_asset_transaction_type') }}
),

final AS (
    SELECT
        bk_fa_transaction_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_fixed_asset_transaction_type
)

SELECT * FROM final