{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_cib_team', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_CIB_TEAM',
        'target_table': 'CSF_CIB_TEAM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.904665+00:00'
    }
) }}

WITH 

source_stg_cib_team AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        team_type,
        team_seqid,
        team,
        site_loc,
        team_name,
        fax,
        theater,
        currency,
        erp_team,
        rep
    FROM {{ source('raw', 'stg_cib_team') }}
),

source_csf_team AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        team_type,
        team_seqid,
        team,
        site_loc,
        team_name,
        fax,
        theater,
        currency,
        erp_team,
        rep
    FROM {{ source('raw', 'csf_team') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    team_type,
    team_seqid,
    team,
    site_loc,
    team_name,
    fax,
    theater,
    currency,
    erp_team,
    rep
    FROM source_csf_team
),

final AS (
    SELECT
        source_dml_type,
        ges_update_date,
        refresh_datetime,
        team_type,
        team_seqid,
        team,
        site_loc,
        team_name,
        fax,
        theater,
        currency,
        erp_team,
        rep
    FROM transformed_exptrans
)

SELECT * FROM final