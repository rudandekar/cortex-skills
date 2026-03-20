{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_as_sbscrptn_qt_atstb_split_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_SM_AS_SBSCRPTN_QT_ATSTB_SPLIT_SEP',
        'target_table': 'SM_AS_SBSCRPTN_QT_ATSTB_SPLIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.283391+00:00'
    }
) }}

WITH 

source_sm_as_sbscrptn_qt_atstb_split AS (
    SELECT
        atstb_split_key,
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        bk_as_technology_name,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_subtechnology_name,
        bk_ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_as_sbscrptn_qt_atstb_split') }}
),

final AS (
    SELECT
        atstb_split_key,
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        bk_as_technology_name,
        bk_as_business_service_name,
        bk_as_architecture_name,
        bk_as_subtechnology_name,
        bk_ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_as_sbscrptn_qt_atstb_split
)

SELECT * FROM final