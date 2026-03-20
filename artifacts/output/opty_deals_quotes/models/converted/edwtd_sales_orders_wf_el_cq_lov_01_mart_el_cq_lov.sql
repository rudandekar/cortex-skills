{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cq_lov', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_CQ_LOV',
        'target_table': 'EL_CQ_LOV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.903471+00:00'
    }
) }}

WITH 

source_el_cq_lov AS (
    SELECT
        lov_index,
        lov_type,
        lov_value,
        active,
        update_datetime,
        lov_subtype,
        lov_order,
        lov_sub_category,
        create_datetime
    FROM {{ source('raw', 'el_cq_lov') }}
),

final AS (
    SELECT
        lov_index,
        lov_type,
        lov_value,
        active,
        update_datetime,
        lov_subtype,
        lov_order,
        lov_sub_category,
        create_datetime
    FROM source_el_cq_lov
)

SELECT * FROM final