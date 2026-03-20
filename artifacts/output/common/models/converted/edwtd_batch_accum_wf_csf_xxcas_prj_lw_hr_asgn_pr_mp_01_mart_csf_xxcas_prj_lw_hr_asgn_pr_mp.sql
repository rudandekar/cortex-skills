{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_xxcas_prj_lw_hr_asgn_pr_mp', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_LW_HR_ASGN_PR_MP',
        'target_table': 'CSF_XXCAS_PRJ_LW_HR_ASGN_PR_MP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.906071+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_lw_hr_asgn_pr_mp AS (
    SELECT
        hr_asgn_id_seq,
        sku_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        org_id,
        work_mgr_id,
        nce1_id,
        nce2_id,
        nce3_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_lw_hr_asgn_pr_mp') }}
),

source_csf_xxcas_prj_lw_hr_asn_prj_mp AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        hr_asgn_id_seq,
        sku_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        org_id,
        work_mgr_id,
        nce1_id,
        nce2_id,
        nce3_id
    FROM {{ source('raw', 'csf_xxcas_prj_lw_hr_asn_prj_mp') }}
),

transformed_exp_csf_xxcas_prj_lw_hr_asgn_pr_mp AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    hr_asgn_id_seq,
    sku_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    org_id,
    work_mgr_id,
    nce1_id,
    nce2_id,
    nce3_id
    FROM source_csf_xxcas_prj_lw_hr_asn_prj_mp
),

final AS (
    SELECT
        hr_asgn_id_seq,
        sku_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        org_id,
        work_mgr_id,
        nce1_id,
        nce2_id,
        nce3_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_lw_hr_asgn_pr_mp
)

SELECT * FROM final