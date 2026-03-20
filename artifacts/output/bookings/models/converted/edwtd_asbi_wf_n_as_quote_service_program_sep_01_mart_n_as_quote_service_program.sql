{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_quote_service_program_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_AS_QUOTE_SERVICE_PROGRAM_SEP',
        'target_table': 'N_AS_QUOTE_SERVICE_PROGRAM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.302711+00:00'
    }
) }}

WITH 

source_n_as_quote_service_program AS (
    SELECT
        bk_service_program_name,
        service_program_desc,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd
    FROM {{ source('raw', 'n_as_quote_service_program') }}
),

final AS (
    SELECT
        bk_service_program_name,
        service_program_desc,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd
    FROM source_n_as_quote_service_program
)

SELECT * FROM final