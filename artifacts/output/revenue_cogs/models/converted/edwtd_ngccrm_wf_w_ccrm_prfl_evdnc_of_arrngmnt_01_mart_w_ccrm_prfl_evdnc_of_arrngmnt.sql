{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_prfl_evdnc_of_arrngmnt', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_PRFL_EVDNC_OF_ARRNGMNT',
        'target_table': 'W_CCRM_PRFL_EVDNC_OF_ARRNGMNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.911475+00:00'
    }
) }}

WITH 

source_st_ccrm_profile_eoa AS (
    SELECT
        batch_id,
        eoa_id,
        profile_id,
        fiscal_period,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        eoa_type,
        reason_code,
        comments,
        current_datetime,
        action_code
    FROM {{ source('raw', 'st_ccrm_profile_eoa') }}
),

transformed_exp_w_ccrm_prfl_evdnc_of_arrngmnt AS (
    SELECT
    bk_ccrm_profile_id_int,
    bk_evdnc_of_arrngmnt_cd,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_ccrm_profile_eoa
),

final AS (
    SELECT
        bk_ccrm_profile_id_int,
        bk_evdnc_of_arrngmnt_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exp_w_ccrm_prfl_evdnc_of_arrngmnt
)

SELECT * FROM final