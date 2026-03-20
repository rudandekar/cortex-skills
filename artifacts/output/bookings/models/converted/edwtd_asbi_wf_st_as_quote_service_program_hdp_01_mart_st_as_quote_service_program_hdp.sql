{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_quote_service_program_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QUOTE_SERVICE_PROGRAM_HDP',
        'target_table': 'ST_AS_QUOTE_SERVICE_PROGRAM_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.346831+00:00'
    }
) }}

WITH 

source_ff_as_quote_service_program AS (
    SELECT
        bk_service_program_name,
        service_program_desc,
        source_deleted_flg,
        bk_ss_cd
    FROM {{ source('raw', 'ff_as_quote_service_program') }}
),

transformed_exptrans AS (
    SELECT
    bk_service_program_name,
    service_program_desc,
    source_deleted_flg,
    bk_ss_cd,
    CURRENT_TIMESTAMP() AS createdtm
    FROM source_ff_as_quote_service_program
),

final AS (
    SELECT
        bk_service_program_name,
        service_program_desc,
        source_deleted_flg,
        bk_ss_cd,
        created_dtm
    FROM transformed_exptrans
)

SELECT * FROM final