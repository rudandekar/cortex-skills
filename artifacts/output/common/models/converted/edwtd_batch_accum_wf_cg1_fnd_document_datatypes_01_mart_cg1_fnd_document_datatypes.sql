{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_fnd_document_datatypes', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_FND_DOCUMENT_DATATYPES',
        'target_table': 'CG1_FND_DOCUMENT_DATATYPES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.632321+00:00'
    }
) }}

WITH 

source_cg1_fnd_document_datatypes AS (
    SELECT
        datatype_id,
        language,
        name,
        user_name,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        start_date_active,
        end_date_active,
        source_lang,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'cg1_fnd_document_datatypes') }}
),

source_stg_cg1_fnd_document_datatypes AS (
    SELECT
        datatype_id,
        language1,
        name1,
        user_name,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        start_date_active,
        end_date_active,
        source_lang,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'stg_cg1_fnd_document_datatypes') }}
),

transformed_exptrans AS (
    SELECT
    datatype_id,
    language1,
    name1,
    user_name,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    start_date_active,
    end_date_active,
    source_lang,
    ges_update_date,
    global_name
    FROM source_stg_cg1_fnd_document_datatypes
),

final AS (
    SELECT
        datatype_id,
        language1,
        name1,
        user_name,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        start_date_active,
        end_date_active,
        source_lang,
        ges_update_date,
        global_name
    FROM transformed_exptrans
)

SELECT * FROM final