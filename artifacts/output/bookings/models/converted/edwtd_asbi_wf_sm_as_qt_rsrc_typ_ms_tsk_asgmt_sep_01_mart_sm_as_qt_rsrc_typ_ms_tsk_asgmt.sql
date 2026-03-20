{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_as_qt_rsrc_typ_ms_tsk_asgmt_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_SM_AS_QT_RSRC_TYP_MS_TSK_ASGMT_SEP',
        'target_table': 'SM_AS_QT_RSRC_TYP_MS_TSK_ASGMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.789849+00:00'
    }
) }}

WITH 

source_sm_as_qt_rsrc_typ_ms_tsk_asgmt AS (
    SELECT
        resource_assignment_key,
        sk_task_resource_id_int,
        bk_ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_as_qt_rsrc_typ_ms_tsk_asgmt') }}
),

final AS (
    SELECT
        resource_assignment_key,
        sk_task_resource_id_int,
        bk_ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_as_qt_rsrc_typ_ms_tsk_asgmt
)

SELECT * FROM final