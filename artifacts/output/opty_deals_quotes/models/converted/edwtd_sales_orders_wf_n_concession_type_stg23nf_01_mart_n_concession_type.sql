{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_concession_type_stg23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CONCESSION_TYPE_STG23NF',
        'target_table': 'N_CONCESSION_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.952590+00:00'
    }
) }}

WITH 

source_n_concession_type AS (
    SELECT
        bk_concession_type_name,
        active_flg,
        concession_subtype_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_concession_type') }}
),

final AS (
    SELECT
        bk_concession_type_name,
        active_flg,
        concession_subtype_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_concession_type
)

SELECT * FROM final