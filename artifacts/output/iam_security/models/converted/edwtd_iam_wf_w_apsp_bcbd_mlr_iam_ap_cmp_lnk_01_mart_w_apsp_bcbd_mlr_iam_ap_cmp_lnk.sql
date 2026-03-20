{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_apsp_bcbd_mlr_iam_ap_cmp_lnk', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_APSP_BCBD_MLR_IAM_AP_CMP_LNK',
        'target_table': 'W_APSP_BCBD_MLR_IAM_AP_CMP_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.725402+00:00'
    }
) }}

WITH 

source_w_apsp_bcbd_mlr_iam_ap_cmp_lnk AS (
    SELECT
        iam_application_component_key,
        bcbd_user_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_apsp_bcbd_mlr_iam_ap_cmp_lnk') }}
),

final AS (
    SELECT
        iam_application_component_key,
        bcbd_user_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_apsp_bcbd_mlr_iam_ap_cmp_lnk
)

SELECT * FROM final