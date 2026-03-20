{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_quote_svc_component_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_AS_QUOTE_SVC_COMPONENT_SEP',
        'target_table': 'N_AS_QUOTE_SVC_COMPONENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.061930+00:00'
    }
) }}

WITH 

source_n_as_quote_svc_component AS (
    SELECT
        bk_svc_component_name,
        svc_component_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_deleted_flg,
        bk_ss_cd
    FROM {{ source('raw', 'n_as_quote_svc_component') }}
),

final AS (
    SELECT
        bk_svc_component_name,
        svc_component_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_deleted_flg,
        bk_ss_cd
    FROM source_n_as_quote_svc_component
)

SELECT * FROM final