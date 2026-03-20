{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_sms3_disti_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_SMS3_DISTI_LINK',
        'target_table': 'EX_IAM_EDWTD_DISTRIBUTOR_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.761468+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_distributor_hier AS (
    SELECT
        upper_user_email,
        user_tpe,
        role_id,
        role_name,
        application_id,
        hierarchy_level,
        distributor_id,
        reseller_id,
        enduser_id,
        status,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_distributor_hier') }}
),

source_w_iam_sms3_disti_link AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        sms3_distributor_key,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_sms3_disti_link') }}
),

routed_rtr_w_iam_sms3_disti_link AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'LEVEL1'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'LEVEL2'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM source_w_iam_sms3_disti_link
),

final AS (
    SELECT
        upper_user_email,
        user_tpe,
        role_id,
        role_name,
        application_id,
        hierarchy_level,
        distributor_id,
        reseller_id,
        enduser_id,
        status,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM routed_rtr_w_iam_sms3_disti_link
)

SELECT * FROM final