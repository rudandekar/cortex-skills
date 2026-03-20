{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cs_sales_order_task_cec', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_CS_SALES_ORDER_TASK_CEC',
        'target_table': 'WI_CS_SO_TASK_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.361698+00:00'
    }
) }}

WITH 

source_wi_cs_so_task_header AS (
    SELECT
        bk_cs_sales_order_task_type_cd,
        cs_so_task_status_cd,
        cs_so_task_created_dtm,
        sk_order_task_id_int,
        ss_cd,
        cs_so_task_obsolete_flg,
        cs_sales_order_task_txt,
        order_number,
        ec_header_id,
        erp_header_id,
        ec_line_id
    FROM {{ source('raw', 'wi_cs_so_task_header') }}
),

source_wi_cs_so_task_orderkey AS (
    SELECT
        sales_order_key,
        bk_cs_sales_order_task_type_cd,
        cs_so_task_status_cd,
        cs_so_task_created_dtm,
        sk_order_task_id_int,
        ss_cd,
        cs_so_task_obsolete_flg,
        cs_sales_order_task_txt,
        order_number,
        ec_header_id,
        erp_header_id,
        ec_line_id
    FROM {{ source('raw', 'wi_cs_so_task_orderkey') }}
),

source_st_om_cec_order_tasks AS (
    SELECT
        created_by,
        creation_date,
        ec_header_id,
        ec_line_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        notes,
        obsolete_flag,
        order_task_id,
        original_system_reference,
        original_system_source_name,
        task_id,
        task_status_code,
        task_text,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cec_order_tasks') }}
),

final AS (
    SELECT
        cs_sales_order_task_key,
        sales_order_key,
        bk_cs_sales_order_task_type_cd,
        cs_so_task_status_cd,
        cs_so_task_created_dtm,
        sk_order_task_id_int,
        ss_cd,
        cs_so_task_obsolete_flg,
        cs_sales_order_task_txt,
        order_number,
        ec_header_id,
        erp_header_id,
        ec_line_id,
        erp_line_id
    FROM source_st_om_cec_order_tasks
)

SELECT * FROM final