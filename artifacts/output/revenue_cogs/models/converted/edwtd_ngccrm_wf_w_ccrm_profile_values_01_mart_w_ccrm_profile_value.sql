{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_profile_values', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_PROFILE_VALUES',
        'target_table': 'W_CCRM_PROFILE_VALUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.204375+00:00'
    }
) }}

WITH 

source_st_ccrm_profile_values AS (
    SELECT
        batch_id,
        profile_id,
        profile_type,
        deal_value,
        last_updated_by,
        last_update_date,
        creation_datetime,
        action_code
    FROM {{ source('raw', 'st_ccrm_profile_values') }}
),

final AS (
    SELECT
        bk_ccrm_profile_id_int,
        bk_ccrm_element_type_cd,
        ccrm_profile_value_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ccrm_profile_values
)

SELECT * FROM final