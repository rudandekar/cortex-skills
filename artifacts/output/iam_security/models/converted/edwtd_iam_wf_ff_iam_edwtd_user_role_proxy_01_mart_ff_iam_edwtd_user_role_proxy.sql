{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_user_role_proxy', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_USER_ROLE_PROXY',
        'target_table': 'FF_IAM_EDWTD_USER_ROLE_PROXY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.771094+00:00'
    }
) }}

WITH 

source_iam_edwtd_user_role_proxy AS (
    SELECT
        proxy_iam_user_id,
        role_name,
        iam_user_id,
        proxy_type_code,
        effective_date,
        end_date,
        created_by,
        create_date,
        updated_by,
        update_date
    FROM {{ source('raw', 'iam_edwtd_user_role_proxy') }}
),

transformed_exp_iam_edwtd_usr_role_acc_hier AS (
    SELECT
    iam_user_id,
    'I' AS action_code,
    ''BatchId'' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_edwtd_user_role_proxy
),

final AS (
    SELECT
        proxy_iam_user_id,
        role_name,
        iam_user_id,
        proxy_type_code,
        effective_date,
        end_date,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM transformed_exp_iam_edwtd_usr_role_acc_hier
)

SELECT * FROM final