{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cq_lov', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CQ_LOV',
        'target_table': 'ST_CQ_LOV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.974429+00:00'
    }
) }}

WITH 

source_ff_cq_lov AS (
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
    FROM {{ source('raw', 'ff_cq_lov') }}
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
    FROM source_ff_cq_lov
)

SELECT * FROM final