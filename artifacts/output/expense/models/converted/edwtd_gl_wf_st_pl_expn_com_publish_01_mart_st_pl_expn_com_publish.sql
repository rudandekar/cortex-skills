{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_pl_expn_com_publish', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_PL_EXPN_COM_PUBLISH',
        'target_table': 'ST_PL_EXPN_COM_PUBLISH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.708337+00:00'
    }
) }}

WITH 

source_ff_pl_expn_com_publish AS (
    SELECT
        batch_id,
        dept_id,
        account_id,
        m1,
        m2,
        m3,
        m4,
        m5,
        m6,
        m7,
        m8,
        m9,
        m10,
        m11,
        m12,
        refresh_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_pl_expn_com_publish') }}
),

final AS (
    SELECT
        batch_id,
        dept_id,
        account_id,
        m1,
        m2,
        m3,
        m4,
        m5,
        m6,
        m7,
        m8,
        m9,
        m10,
        m11,
        m12,
        refresh_date,
        create_datetime,
        action_code
    FROM source_ff_pl_expn_com_publish
)

SELECT * FROM final