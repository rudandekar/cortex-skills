{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_iam_user_role_app_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_WI_IAM_USER_ROLE_APP_MAP',
        'target_table': 'WI_IAM_USER_ROLE_APP_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.723172+00:00'
    }
) }}

WITH 

source_n_iam_ptnrs_hier_lnk AS (
    SELECT
        partner_site_party_key,
        bk_iam_role_name,
        iam_application_key,
        iam_user_key,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        dd_bk_prtnr_site_party_id_int,
        iam_level_num_int,
        source_deleted_flg,
        dd_cec_id,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_iam_ptnrs_hier_lnk') }}
),

final AS (
    SELECT
        cec_id,
        application_name,
        dv_user_partner_cnt,
        iam_application_key,
        iam_user_key
    FROM source_n_iam_ptnrs_hier_lnk
)

SELECT * FROM final