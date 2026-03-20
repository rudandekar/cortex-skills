{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_biz_data_attrib', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_BIZ_DATA_ATTRIB',
        'target_table': 'FF_IAM_EDWTD_BIZ_DATA_ATTRIB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.718253+00:00'
    }
) }}

WITH 

source_iam_edwtd_biz_data_attrib_vw AS (
    SELECT
        biz_data_attrib_name,
        biz_data_attrib_descr,
        status,
        created_by,
        create_date,
        updated_by,
        update_date
    FROM {{ source('raw', 'iam_edwtd_biz_data_attrib_vw') }}
),

transformed_exp_ff_iam_edwtd_biz_data_attrib AS (
    SELECT
    biz_data_attrib_name,
    biz_data_attrib_descr,
    status,
    created_by,
    create_date,
    updated_by,
    update_date,
    'I' AS action_code,
    'BatchId' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_edwtd_biz_data_attrib_vw
),

final AS (
    SELECT
        biz_data_attrib_name,
        biz_data_attrib_descr,
        status,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM transformed_exp_ff_iam_edwtd_biz_data_attrib
)

SELECT * FROM final