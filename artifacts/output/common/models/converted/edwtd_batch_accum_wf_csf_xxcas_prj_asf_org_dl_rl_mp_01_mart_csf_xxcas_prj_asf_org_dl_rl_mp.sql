{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_xxcas_prj_asf_org_dl_rl_mp', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_ASF_ORG_DL_RL_MP',
        'target_table': 'CSF_XXCAS_PRJ_ASF_ORG_DL_RL_MP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.515562+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_asf_org_dl_rl_mp AS (
    SELECT
        map_id_seq,
        proj_mgr,
        delivery_mgr,
        fin_analyst,
        prj_coordinator,
        work_mgr,
        nce1,
        nce2,
        nce3,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        effective_start_date,
        effective_end_date,
        prj_owning_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_asf_org_dl_rl_mp') }}
),

source_csf_xxcas_prj_asf_org_dl_rl_mp AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        map_id_seq,
        proj_mgr,
        delivery_mgr,
        fin_analyst,
        prj_coordinator,
        work_mgr,
        nce1,
        nce2,
        nce3,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        effective_start_date,
        effective_end_date,
        prj_owning_id
    FROM {{ source('raw', 'csf_xxcas_prj_asf_org_dl_rl_mp') }}
),

transformed_exp_csf_xxcas_prj_asf_org_dl_rl_mp AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    map_id_seq,
    proj_mgr,
    delivery_mgr,
    fin_analyst,
    prj_coordinator,
    work_mgr,
    nce1,
    nce2,
    nce3,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    effective_start_date,
    effective_end_date,
    prj_owning_id
    FROM source_csf_xxcas_prj_asf_org_dl_rl_mp
),

final AS (
    SELECT
        map_id_seq,
        proj_mgr,
        delivery_mgr,
        fin_analyst,
        prj_coordinator,
        work_mgr,
        nce1,
        nce2,
        nce3,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        effective_start_date,
        effective_end_date,
        prj_owning_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_asf_org_dl_rl_mp
)

SELECT * FROM final