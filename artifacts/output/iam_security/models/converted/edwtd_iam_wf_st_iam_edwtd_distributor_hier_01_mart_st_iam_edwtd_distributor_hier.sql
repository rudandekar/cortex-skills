{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_iam_edwtd_distributor_hier', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_ST_IAM_EDWTD_DISTRIBUTOR_HIER',
        'target_table': 'ST_IAM_EDWTD_DISTRIBUTOR_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.734932+00:00'
    }
) }}

WITH 

source_ff_iam_edwtd_distibutor_hier AS (
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
    FROM {{ source('raw', 'ff_iam_edwtd_distibutor_hier') }}
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
    FROM source_ff_iam_edwtd_distibutor_hier
)

SELECT * FROM final