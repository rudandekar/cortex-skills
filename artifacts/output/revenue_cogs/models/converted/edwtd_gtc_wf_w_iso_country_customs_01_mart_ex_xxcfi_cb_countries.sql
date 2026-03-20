{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iso_country_customs', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_W_ISO_COUNTRY_CUSTOMS',
        'target_table': 'EX_XXCFI_CB_COUNTRIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.048423+00:00'
    }
) }}

WITH 

source_ex_xxcfi_cb_countries AS (
    SELECT
        batch_id,
        country_id,
        country_code,
        country_name,
        description,
        created_by,
        created_date,
        modified_by,
        modified_date,
        interface_items,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_xxcfi_cb_countries') }}
),

source_w_iso_country_customs AS (
    SELECT
        bk_iso_country_cd,
        bk_world_country_group_cd,
        override_iso_country_cd,
        iso_country_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iso_country_customs') }}
),

final AS (
    SELECT
        batch_id,
        country_id,
        country_code,
        country_name,
        description,
        created_by,
        created_date,
        modified_by,
        modified_date,
        interface_items,
        create_datetime,
        action_code,
        exception_type
    FROM source_w_iso_country_customs
)

SELECT * FROM final