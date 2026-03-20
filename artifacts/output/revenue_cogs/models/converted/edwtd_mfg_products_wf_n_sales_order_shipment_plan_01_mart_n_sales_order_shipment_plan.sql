{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_shipment_plan', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_SHIPMENT_PLAN',
        'target_table': 'N_SALES_ORDER_SHIPMENT_PLAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.165156+00:00'
    }
) }}

WITH 

source_w_sales_order_shipment_plan AS (
    SELECT
        bk_shipset_num_int,
        sales_order_key,
        sol_shipment_delivery_key,
        planning_status_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_shipment_plan') }}
),

final AS (
    SELECT
        bk_shipset_num_int,
        sales_order_key,
        sol_shipment_delivery_key,
        planning_status_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_sales_order_shipment_plan
)

SELECT * FROM final