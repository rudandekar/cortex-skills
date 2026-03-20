{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_phx_sc_team', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_PHX_SC_TEAM',
        'target_table': 'ST_OTM_PHX_SC_TEAM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.867086+00:00'
    }
) }}

WITH 

source_xxotm_phx_sc_team AS (
    SELECT
        sc_agent_id,
        sc_id,
        shr_team_name,
        shr_team_salesrep_number,
        shr_team_agent_id,
        team_type,
        shr_ind_salesrep_number,
        shr_ind_salesrep_id,
        shr_ind_split_percent,
        otm_batch_id,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'xxotm_phx_sc_team') }}
),

final AS (
    SELECT
        sc_agent_id,
        sc_id,
        shr_team_name,
        shr_team_salesrep_number,
        shr_team_agent_id,
        team_type,
        shr_ind_salesrep_number,
        shr_ind_salesrep_id,
        shr_ind_split_percent,
        otm_batch_id,
        object_version_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_xxotm_phx_sc_team
)

SELECT * FROM final