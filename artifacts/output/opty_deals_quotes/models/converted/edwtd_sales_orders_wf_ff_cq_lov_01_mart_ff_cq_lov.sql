{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cq_lov', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_FF_CQ_LOV',
        'target_table': 'FF_CQ_LOV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.982497+00:00'
    }
) }}

WITH 

source_cq_lov AS (
    SELECT
        lov_index,
        lov_type,
        lov_value,
        active,
        created_by,
        created_on,
        updated_by,
        updated_on,
        lov_subtype,
        lov_order,
        lov_sub_category
    FROM {{ source('raw', 'cq_lov') }}
),

transformed_exp_cq_lov AS (
    SELECT
    lov_index,
    lov_type,
    lov_value,
    active,
    created_by,
    created_on,
    updated_by,
    updated_on,
    lov_subtype,
    lov_order,
    lov_sub_category,
    'BATCHID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_cq_lov
),

final AS (
    SELECT
        batch_id,
        lov_index,
        lov_type,
        lov_value,
        active,
        created_by,
        created_on,
        updated_by,
        updated_on,
        lov_subtype,
        lov_order,
        lov_sub_category,
        create_datetime,
        action_code
    FROM transformed_exp_cq_lov
)

SELECT * FROM final