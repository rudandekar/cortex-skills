{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ic_transfer_price_method_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_IC_TRANSFER_PRICE_METHOD_STG23NF',
        'target_table': 'N_IC_TRANSFER_PRICE_METHOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.439824+00:00'
    }
) }}

WITH 

source_n_ic_transfer_price_method AS (
    SELECT
        bk_transfer_price_method_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_ic_transfer_price_method') }}
),

final AS (
    SELECT
        bk_transfer_price_method_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_ic_transfer_price_method
)

SELECT * FROM final