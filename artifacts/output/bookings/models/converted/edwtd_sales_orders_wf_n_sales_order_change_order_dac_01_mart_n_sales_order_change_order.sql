{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_change_order_dac', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_CHANGE_ORDER_DAC',
        'target_table': 'N_SALES_ORDER_CHANGE_ORDER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.912455+00:00'
    }
) }}

WITH 

source_n_sales_order_change_order AS (
    SELECT
        so_change_order_key,
        sk_xxccp_hdr_id_indctr_int,
        status_cd,
        sub_status_cd,
        change_type_cd,
        sales_order_key,
        change_order_creation_dtm,
        dv_change_order_creation_dt,
        change_order_last_update_dtm,
        dv_change_order_last_update_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_sales_order_change_order') }}
),

final AS (
    SELECT
        so_change_order_key,
        sk_xxccp_hdr_id_indctr_int,
        status_cd,
        sub_status_cd,
        change_type_cd,
        sales_order_key,
        change_order_creation_dtm,
        dv_change_order_creation_dt,
        change_order_last_update_dtm,
        dv_change_order_last_update_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_sales_order_change_order
)

SELECT * FROM final