{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_co_dt_steps', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_CO_DT_STEPS',
        'target_table': 'EL_CO_DT_STEPS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.687046+00:00'
    }
) }}

WITH 

source_el_co_dt_steps AS (
    SELECT
        step_id,
        type_1,
        type_seq,
        step_desc,
        rule,
        status,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        step_dtl,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'el_co_dt_steps') }}
),

final AS (
    SELECT
        step_id,
        type_1,
        type_seq,
        step_desc,
        rule,
        status,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        step_dtl,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_el_co_dt_steps
)

SELECT * FROM final