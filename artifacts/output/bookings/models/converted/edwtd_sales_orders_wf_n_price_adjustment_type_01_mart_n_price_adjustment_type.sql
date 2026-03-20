{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_price_adjustment_type', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_PRICE_ADJUSTMENT_TYPE',
        'target_table': 'N_PRICE_ADJUSTMENT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.944733+00:00'
    }
) }}

WITH 

source_w_price_adjustment_type AS (
    SELECT
        bk_price_adjustment_type_cd,
        price_adjustment_type_name,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_price_adjustment_type') }}
),

final AS (
    SELECT
        bk_price_adjustment_type_cd,
        price_adjustment_type_name,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_price_adjustment_type
)

SELECT * FROM final