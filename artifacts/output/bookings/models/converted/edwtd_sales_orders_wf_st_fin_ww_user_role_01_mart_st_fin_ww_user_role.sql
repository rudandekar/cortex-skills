{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fin_ww_user_role', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_FIN_WW_USER_ROLE',
        'target_table': 'ST_FIN_WW_USER_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.376712+00:00'
    }
) }}

WITH 

source_xxanp_fin_ww_user_role AS (
    SELECT
        cec_id,
        user_role,
        active_flag,
        last_update_date
    FROM {{ source('raw', 'xxanp_fin_ww_user_role') }}
),

final AS (
    SELECT
        cec_id,
        user_role,
        active_flag,
        last_update_date,
        edw_create_user,
        edw_create_dtm
    FROM source_xxanp_fin_ww_user_role
)

SELECT * FROM final