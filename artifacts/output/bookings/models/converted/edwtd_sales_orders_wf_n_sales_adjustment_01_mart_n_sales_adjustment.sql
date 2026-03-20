{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_adjustment', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ADJUSTMENT',
        'target_table': 'N_SALES_ADJUSTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.389407+00:00'
    }
) }}

WITH 

source_n_sales_adjustment_nrt AS (
    SELECT
        bk_sales_adj_number_int,
        sales_adjustment_source_code,
        sales_adj_physical_source_cd,
        bk_sales_ide_adj_code,
        bk_sales_adj_code,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        bk_sales_adj_type_code
    FROM {{ source('raw', 'n_sales_adjustment_nrt') }}
),

final AS (
    SELECT
        bk_sales_adj_number_int,
        sales_adjustment_source_code,
        sales_adj_physical_source_cd,
        bk_sales_ide_adj_code,
        bk_sales_adj_code,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        bk_sales_adj_type_code
    FROM source_n_sales_adjustment_nrt
)

SELECT * FROM final