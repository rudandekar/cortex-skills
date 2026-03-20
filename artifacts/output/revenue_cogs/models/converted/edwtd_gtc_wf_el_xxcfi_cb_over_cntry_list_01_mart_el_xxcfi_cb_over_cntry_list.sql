{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_xxcfi_cb_over_cntry_list', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_XXCFI_CB_OVER_CNTRY_LIST',
        'target_table': 'EL_XXCFI_CB_OVER_CNTRY_LIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.666099+00:00'
    }
) }}

WITH 

source_st_xxcfi_cb_over_cntry_list AS (
    SELECT
        batch_id,
        override_id,
        ship_to_country,
        override_country_group_id,
        override_country_group_code,
        override_country_code,
        comments,
        active_flag,
        start_date,
        end_date,
        created_by,
        created_date,
        modified_by,
        modified_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcfi_cb_over_cntry_list') }}
),

final AS (
    SELECT
        override_id,
        ship_to_country,
        override_country_group_code,
        override_country_code,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_st_xxcfi_cb_over_cntry_list
)

SELECT * FROM final