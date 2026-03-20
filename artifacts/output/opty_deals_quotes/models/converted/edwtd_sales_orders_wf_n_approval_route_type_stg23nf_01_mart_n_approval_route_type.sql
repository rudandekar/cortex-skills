{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_approval_route_type_stg23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_APPROVAL_ROUTE_TYPE_STG23NF',
        'target_table': 'N_APPROVAL_ROUTE_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.974635+00:00'
    }
) }}

WITH 

source_n_approval_route_type AS (
    SELECT
        bk_approval_route_type_name,
        active_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_approval_route_type') }}
),

final AS (
    SELECT
        bk_approval_route_type_name,
        active_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_approval_route_type
)

SELECT * FROM final