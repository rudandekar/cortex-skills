{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_bv_fin_sls_theater_hier', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_BV_FIN_SLS_THEATER_HIER',
        'target_table': 'N_IAM_BV_FIN_SLS_THEATER_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.716235+00:00'
    }
) }}

WITH 

source_w_iam_bv_fin_sls_theater_hier AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        sales_territory_key,
        iam_application_key,
        standard_or_exception_flg,
        assignment_type_cd,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_bv_fin_sls_theater_hier') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        sales_territory_key,
        iam_application_key,
        standard_or_exception_flg,
        assignment_type_cd,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_iam_bv_fin_sls_theater_hier
)

SELECT * FROM final