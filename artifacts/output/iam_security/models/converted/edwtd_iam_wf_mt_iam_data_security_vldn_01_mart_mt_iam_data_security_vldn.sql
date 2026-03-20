{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_iam_data_security_vldn', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_IAM_DATA_SECURITY_VLDN',
        'target_table': 'MT_IAM_DATA_SECURITY_VLDN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.754847+00:00'
    }
) }}

WITH 

source_mt_iam_data_security_vldn AS (
    SELECT
        cec_id,
        hier_type_descr,
        dv_application_name,
        dv_level_0_flg,
        dv_level_1_flg,
        dv_level_2_flg,
        dv_level_3_flg,
        dv_level_4_flg,
        dv_level_5_flg,
        dv_level_6_flg,
        dv_level_7_flg,
        dv_level_8_flg,
        dv_level_9_flg,
        dv_level_10_flg,
        dv_max_lvl_cd,
        dv_min_lvl_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_iam_data_security_vldn') }}
),

final AS (
    SELECT
        cec_id,
        hier_type_descr,
        dv_application_name,
        dv_level_0_flg,
        dv_level_1_flg,
        dv_level_2_flg,
        dv_level_3_flg,
        dv_level_4_flg,
        dv_level_5_flg,
        dv_level_6_flg,
        dv_level_7_flg,
        dv_level_8_flg,
        dv_level_9_flg,
        dv_level_10_flg,
        dv_max_lvl_cd,
        dv_min_lvl_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_iam_data_security_vldn
)

SELECT * FROM final