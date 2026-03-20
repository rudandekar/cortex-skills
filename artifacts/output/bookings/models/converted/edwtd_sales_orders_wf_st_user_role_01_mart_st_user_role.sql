{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_user_role', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_USER_ROLE',
        'target_table': 'ST_USER_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.325141+00:00'
    }
) }}

WITH 

source_st_fin_ww_user_role AS (
    SELECT
        cec_id,
        cisco_worker_party_key,
        user_role,
        active_flag,
        last_update_date
    FROM {{ source('raw', 'st_fin_ww_user_role') }}
),

final AS (
    SELECT
        cec_id,
        cisco_worker_party_key,
        user_role,
        active_flag,
        last_update_date,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_st_fin_ww_user_role
)

SELECT * FROM final