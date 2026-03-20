{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_srce_type', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_SRCE_TYPE',
        'target_table': 'W_SALES_ORDER_SRCE_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.176199+00:00'
    }
) }}

WITH 

source_st_uo_oe_order_sources AS (
    SELECT
        creation_date,
        description,
        enabled_flag,
        name,
        order_source_id,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        batch_id,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_uo_oe_order_sources') }}
),

transformed_exp_uo_w_sales_order_srce_type AS (
    SELECT
    so_source_description,
    so_source_enabled_flag,
    bk_so_source_name,
    order_source_id,
    create_datetime,
    batch_id,
    action_code,
    exception_type,
    rank_index,
    dml_type,
    end_tv_date
    FROM source_st_uo_oe_order_sources
),

final AS (
    SELECT
        bk_so_source_name,
        start_tv_date,
        end_tv_date,
        so_source_description,
        so_source_enabled_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime,
        action_code,
        dml_type
    FROM transformed_exp_uo_w_sales_order_srce_type
)

SELECT * FROM final