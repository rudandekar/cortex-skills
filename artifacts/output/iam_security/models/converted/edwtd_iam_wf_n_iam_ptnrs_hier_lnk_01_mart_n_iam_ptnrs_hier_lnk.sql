{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_ptnrs_hier_lnk', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_PTNRS_HIER_LNK',
        'target_table': 'N_IAM_PTNRS_HIER_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.713832+00:00'
    }
) }}

WITH 

source_w_iam_ptnrs_hier_lnk AS (
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
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_ptnrs_hier_lnk') }}
),

final AS (
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
    FROM source_w_iam_ptnrs_hier_lnk
)

SELECT * FROM final