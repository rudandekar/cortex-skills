{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_emp_recog_reward_type_stg23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EMP_RECOG_REWARD_TYPE_STG23NF',
        'target_table': 'N_EMP_RECOG_REWARD_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.899572+00:00'
    }
) }}

WITH 

source_st_emp_recog_reward_type AS (
    SELECT
        emp_recog_reward_type_cd,
        source_deleted_flg,
        action_cd,
        create_datetime
    FROM {{ source('raw', 'st_emp_recog_reward_type') }}
),

final AS (
    SELECT
        bk_emp_recog_reward_type_cd,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_emp_recog_reward_type
)

SELECT * FROM final