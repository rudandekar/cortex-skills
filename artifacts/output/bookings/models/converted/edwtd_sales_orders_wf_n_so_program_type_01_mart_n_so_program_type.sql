{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_so_program_type', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SO_PROGRAM_TYPE',
        'target_table': 'N_SO_PROGRAM_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.988329+00:00'
    }
) }}

WITH 

source_w_so_program_type AS (
    SELECT
        bk_so_program_type_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_so_program_type') }}
),

final AS (
    SELECT
        bk_so_program_type_name,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_so_program_type
)

SELECT * FROM final