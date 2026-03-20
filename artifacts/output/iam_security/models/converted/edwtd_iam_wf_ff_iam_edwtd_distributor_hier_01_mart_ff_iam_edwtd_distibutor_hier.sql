{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_distributor_hier', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_DISTRIBUTOR_HIER',
        'target_table': 'FF_IAM_EDWTD_DISTIBUTOR_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.717093+00:00'
    }
) }}

WITH 

source_iam_distributor_hier_vw AS (
    SELECT
        upper_user_email,
        user_type,
        role_id,
        role_name,
        application_id,
        hierarchy_level,
        distributor_id,
        reseller_id,
        enduser_id,
        status,
        create_by,
        create_date,
        update_by,
        update_date
    FROM {{ source('raw', 'iam_distributor_hier_vw') }}
),

transformed_exp_iam_edwtd_usr_role_acc_hier AS (
    SELECT
    iam_user_id,
    'I' AS action_code,
    ''BatchId'' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_distributor_hier_vw
),

final AS (
    SELECT
        upper_user_email,
        user_type,
        role_id,
        role_name,
        application_id,
        hierarchy_level,
        distributor_id,
        reseller_id,
        enduser_id,
        status,
        create_by,
        create_date,
        update_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM transformed_exp_iam_edwtd_usr_role_acc_hier
)

SELECT * FROM final