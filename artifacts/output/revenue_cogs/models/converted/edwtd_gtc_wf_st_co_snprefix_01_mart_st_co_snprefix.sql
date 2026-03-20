{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_co_snprefix', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CO_SNPREFIX',
        'target_table': 'ST_CO_SNPREFIX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.651463+00:00'
    }
) }}

WITH 

source_ff_co_snprefix_incr AS (
    SELECT
        batch_id,
        snprefix_id,
        project_id,
        snprefix,
        coo,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_co_snprefix_incr') }}
),

final AS (
    SELECT
        batch_id,
        snprefix_id,
        project_id,
        snprefix,
        coo,
        created_by,
        created_date,
        last_updated_by,
        last_updated_date,
        create_datetime,
        action_code
    FROM source_ff_co_snprefix_incr
)

SELECT * FROM final