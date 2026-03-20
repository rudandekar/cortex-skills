{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_as_quote_svc_component_hdp', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_ST_AS_QUOTE_SVC_COMPONENT_HDP',
        'target_table': 'ST_AS_QUOTE_SVC_COMPONENT_HDP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.281635+00:00'
    }
) }}

WITH 

source_ff_as_quote_svc_component AS (
    SELECT
        bk_svc_component_name,
        svc_component_descr,
        source_deleted_flg,
        bk_ss_cd
    FROM {{ source('raw', 'ff_as_quote_svc_component') }}
),

transformed_exptrans AS (
    SELECT
    bk_svc_component_name,
    svc_component_descr,
    source_deleted_flg,
    bk_ss_cd,
    CURRENT_TIMESTAMP() AS createdtm
    FROM source_ff_as_quote_svc_component
),

final AS (
    SELECT
        bk_svc_component_name,
        svc_component_descr,
        source_deleted_flg,
        bk_ss_cd,
        created_dtm
    FROM transformed_exptrans
)

SELECT * FROM final