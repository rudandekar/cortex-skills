{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_dwh_cisco_subscr_lines_details', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_DWH_CISCO_SUBSCR_LINES_DETAILS',
        'target_table': 'STG_CISCO_SUBSCR_LINES_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.960117+00:00'
    }
) }}

WITH 

source_cisco_subscr_lines_details AS (
    SELECT
        obj_id0,
        subscription_code,
        ccw_order_line_id,
        item_id,
        subscription_renew_cnt,
        subscr_credit,
        currency_code,
        subscr_monthly_credit,
        subscription_line_start_dt,
        subscription_line_end_dt,
        end_complete_date,
        status,
        quantity,
        unique_id,
        instance_id,
        intended_use,
        web_order_id,
        linked_line_ref_number,
        last_modified_date,
        source_commit_time,
        refresh_datetime,
        source_dml_type,
        fully_qualified_table_name,
        trail_position,
        token,
        refresh_day,
        flexup_sku,
        strategic_class,
        strategic_intent
    FROM {{ source('raw', 'cisco_subscr_lines_details') }}
),

source_stg_cisco_subscr_lines_details AS (
    SELECT
        obj_id0,
        subscription_code,
        ccw_order_line_id,
        item_id,
        subscription_renew_cnt,
        subscr_credit,
        currency_code,
        subscr_monthly_credit,
        subscription_line_start_dt,
        subscription_line_end_dt,
        end_complete_date,
        status,
        quantity,
        unique_id,
        instance_id,
        intended_use,
        web_order_id,
        linked_line_ref_number,
        last_modified_date,
        source_commit_time,
        refresh_datetime,
        source_dml_type,
        fully_qualified_table_name,
        trail_position,
        token,
        refresh_day,
        flexup_flag,
        strategic_class,
        strategic_intent
    FROM {{ source('raw', 'stg_cisco_subscr_lines_details') }}
),

transformed_ex_cisco_subscr_lines_details AS (
    SELECT
    obj_id0,
    subscription_code,
    ccw_order_line_id,
    item_id,
    subscription_renew_cnt,
    subscr_credit,
    currency_code,
    subscr_monthly_credit,
    subscription_line_start_dt,
    subscription_line_end_dt,
    end_complete_date,
    status,
    quantity,
    unique_id,
    instance_id,
    intended_use,
    web_order_id,
    linked_line_ref_number,
    last_modified_date,
    source_commit_time,
    refresh_datetime,
    source_dml_type,
    fully_qualified_table_name,
    trail_position,
    token,
    refresh_day,
    flexup_sku,
    strategic_class,
    strategic_intent
    FROM source_stg_cisco_subscr_lines_details
),

final AS (
    SELECT
        obj_id0,
        subscription_code,
        ccw_order_line_id,
        item_id,
        subscription_renew_cnt,
        subscr_credit,
        currency_code,
        subscr_monthly_credit,
        subscription_line_start_dt,
        subscription_line_end_dt,
        end_complete_date,
        status,
        quantity,
        unique_id,
        instance_id,
        intended_use,
        web_order_id,
        linked_line_ref_number,
        last_modified_date,
        source_commit_time,
        refresh_datetime,
        source_dml_type,
        fully_qualified_table_name,
        trail_position,
        token,
        refresh_day,
        flexup_flag,
        strategic_class,
        strategic_intent
    FROM transformed_ex_cisco_subscr_lines_details
)

SELECT * FROM final