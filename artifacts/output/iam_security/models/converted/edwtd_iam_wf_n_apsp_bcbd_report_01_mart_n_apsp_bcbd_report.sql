{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_apsp_bcbd_report', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_APSP_BCBD_REPORT',
        'target_table': 'N_APSP_BCBD_REPORT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.749077+00:00'
    }
) }}

WITH 

source_st_apsp_bcbd_report AS (
    SELECT
        bk_report_cd,
        bk_report_role_name,
        report_name,
        report_metric_name,
        subject_area_name,
        report_created_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'st_apsp_bcbd_report') }}
),

transformed_exp_report_dim_load AS (
    SELECT
    bk_report_cd,
    bk_report_role_name,
    report_name,
    report_metric_name,
    subject_area_name,
    report_created_dt,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user
    FROM source_st_apsp_bcbd_report
),

final AS (
    SELECT
        bk_report_cd,
        bk_report_role_name,
        report_name,
        report_metric_name,
        subject_area_name,
        report_created_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM transformed_exp_report_dim_load
)

SELECT * FROM final