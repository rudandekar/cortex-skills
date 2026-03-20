{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_hold_group', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_HOLD_GROUP',
        'target_table': 'W_SALES_ORDER_HOLD_GROUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.848729+00:00'
    }
) }}

WITH 

source_st_uo_oe_hold_definitions AS (
    SELECT
        hold_id,
        activity_name,
        name,
        attribute2,
        description,
        type_code,
        attribute6,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        create_datetime
    FROM {{ source('raw', 'st_uo_oe_hold_definitions') }}
),

final AS (
    SELECT
        bk_sol_hold_group_code,
        start_tv_date,
        end_tv_date,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        action_code,
        dml_type
    FROM source_st_uo_oe_hold_definitions
)

SELECT * FROM final