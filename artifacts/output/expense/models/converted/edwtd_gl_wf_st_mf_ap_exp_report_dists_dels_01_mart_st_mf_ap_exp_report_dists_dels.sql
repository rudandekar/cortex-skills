{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_ap_exp_report_dists_dels', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_AP_EXP_REPORT_DISTS_DELS',
        'target_table': 'ST_MF_AP_EXP_REPORT_DISTS_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.852342+00:00'
    }
) }}

WITH 

source_ff_mf_ap_exp_report_dists_dels AS (
    SELECT
        batch_id,
        report_header_id,
        report_line_id,
        report_distribution_id,
        org_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        code_combination_id,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        segment7,
        segment8,
        segment9,
        segment10,
        amount,
        project_id,
        task_id,
        award_id,
        expenditure_organization_id,
        cost_center,
        ges_update_date,
        ges_delete_date,
        global_name,
        action_code,
        create_datetime
    FROM {{ source('raw', 'ff_mf_ap_exp_report_dists_dels') }}
),

final AS (
    SELECT
        batch_id,
        report_header_id,
        report_line_id,
        report_distribution_id,
        org_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        code_combination_id,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        segment7,
        segment8,
        segment9,
        segment10,
        amount,
        project_id,
        task_id,
        award_id,
        expenditure_organization_id,
        cost_center,
        ges_update_date,
        ges_delete_date,
        global_name,
        action_code,
        create_datetime
    FROM source_ff_mf_ap_exp_report_dists_dels
)

SELECT * FROM final