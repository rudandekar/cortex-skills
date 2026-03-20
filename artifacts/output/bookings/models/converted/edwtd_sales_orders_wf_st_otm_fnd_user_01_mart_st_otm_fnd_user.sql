{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_fnd_user', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_FND_USER',
        'target_table': 'ST_OTM_FND_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.900164+00:00'
    }
) }}

WITH 

source_st_otm_fnd_user AS (
    SELECT
        cdb_dequeue_time,
        employee_number,
        etl_process_date,
        last_update_date,
        user_id,
        user_name
    FROM {{ source('raw', 'st_otm_fnd_user') }}
),

final AS (
    SELECT
        cdb_dequeue_time,
        employee_number,
        etl_process_date,
        last_update_date,
        user_id,
        user_name
    FROM source_st_otm_fnd_user
)

SELECT * FROM final