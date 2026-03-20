{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_apsp_bcbd_report', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_ST_APSP_BCBD_REPORT',
        'target_table': 'ST_APSP_BCBD_REPORT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.738570+00:00'
    }
) }}

WITH 

source_ff_s_ca_fin_report_dim AS (
    SELECT
        report_id,
        report_name,
        role_name,
        metrics,
        created_date,
        subject_area_name
    FROM {{ source('raw', 'ff_s_ca_fin_report_dim') }}
),

transformed_exp_report_dim_load AS (
    SELECT
    report_id,
    report_name,
    role_name,
    metrics,
    created_date,
    subject_area_name
    FROM source_ff_s_ca_fin_report_dim
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