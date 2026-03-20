{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ngccrm_subreason_codes', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_NGCCRM_SUBREASON_CODES',
        'target_table': 'FF_NGCCRM_SUBREASON_CODES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.430906+00:00'
    }
) }}

WITH 

source_ngccrm_subreason_codes AS (
    SELECT
        src_id,
        src_name,
        version,
        description,
        start_date,
        end_date,
        comments,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        latest_flag,
        status
    FROM {{ source('raw', 'ngccrm_subreason_codes') }}
),

transformed_exp_ngccrm_subreason_codes AS (
    SELECT
    src_id,
    src_name,
    version,
    description,
    start_date,
    end_date,
    comments,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    latest_flag,
    status,
    'BATCH_ID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_timestamp,
    'I' AS action_code
    FROM source_ngccrm_subreason_codes
),

final AS (
    SELECT
        batch_id,
        src_id,
        src_name,
        version,
        description,
        start_date,
        end_date,
        comments,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        latest_flag,
        status,
        create_timestamp,
        action_code
    FROM transformed_exp_ngccrm_subreason_codes
)

SELECT * FROM final