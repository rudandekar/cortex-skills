{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_as_quote_service_deliverable_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_W_AS_QUOTE_SERVICE_DELIVERABLE_SEP',
        'target_table': 'W_AS_QUOTE_SERVICE_DELIVERABLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.874410+00:00'
    }
) }}

WITH 

source_w_as_quote_service_deliverable AS (
    SELECT
        bk_deliverable_name,
        deliverable_desc,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_as_quote_service_deliverable') }}
),

final AS (
    SELECT
        bk_deliverable_name,
        deliverable_desc,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd,
        action_code,
        dml_type
    FROM source_w_as_quote_service_deliverable
)

SELECT * FROM final