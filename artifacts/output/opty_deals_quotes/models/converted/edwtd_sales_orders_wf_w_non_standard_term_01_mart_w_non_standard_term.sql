{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_non_standard_term', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_NON_STANDARD_TERM',
        'target_table': 'W_NON_STANDARD_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.915553+00:00'
    }
) }}

WITH 

source_w_non_standard_term AS (
    SELECT
        bk_non_standard_term_name,
        source_created_dtm,
        source_created_by_user_id,
        source_updated_dtm,
        source_updated_by_user_id,
        sk_nsterm_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_non_standard_term') }}
),

final AS (
    SELECT
        bk_non_standard_term_name,
        source_created_dtm,
        source_created_by_user_id,
        source_updated_dtm,
        source_updated_by_user_id,
        sk_nsterm_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_non_standard_term
)

SELECT * FROM final