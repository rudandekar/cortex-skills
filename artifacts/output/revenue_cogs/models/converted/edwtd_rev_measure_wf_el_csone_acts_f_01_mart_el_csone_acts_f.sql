{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csone_acts_f', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_CSONE_ACTS_F',
        'target_table': 'EL_CSONE_ACTS_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.710965+00:00'
    }
) }}

WITH 

source_st_csone_acts AS (
    SELECT
        user_id,
        user_name,
        login_time,
        login_status
    FROM {{ source('raw', 'st_csone_acts') }}
),

final AS (
    SELECT
        user_id,
        user_name,
        login_time,
        login_status,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_csone_acts
)

SELECT * FROM final