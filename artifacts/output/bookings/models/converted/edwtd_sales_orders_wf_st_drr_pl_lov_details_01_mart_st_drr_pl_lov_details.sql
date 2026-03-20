{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_drr_pl_lov_details', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_DRR_PL_LOV_DETAILS',
        'target_table': 'ST_DRR_PL_LOV_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.359650+00:00'
    }
) }}

WITH 

source_ff_st_drr_pl_lov_details AS (
    SELECT
        lov_category,
        lov_name,
        lov_value,
        attribute3,
        otm_territory_name,
        enabled_flag,
        active_start_date,
        active_end_date,
        node_name
    FROM {{ source('raw', 'ff_st_drr_pl_lov_details') }}
),

final AS (
    SELECT
        lov_category,
        lov_name,
        lov_value,
        attribute3,
        otm_territory_name,
        enabled_flag,
        active_start_date,
        active_end_date,
        node_name
    FROM source_ff_st_drr_pl_lov_details
)

SELECT * FROM final