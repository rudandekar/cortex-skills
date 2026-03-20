{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_pl_expn_bud_publish', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_PL_EXPN_BUD_PUBLISH',
        'target_table': 'EL_PL_EXPN_BUD_PUBLISH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.021524+00:00'
    }
) }}

WITH 

source_st_pl_expn_bud_publish AS (
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
    FROM {{ source('raw', 'st_pl_expn_bud_publish') }}
),

final AS (
    SELECT
        fiscal_year_month_id,
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
        refresh_date
    FROM source_st_pl_expn_bud_publish
)

SELECT * FROM final