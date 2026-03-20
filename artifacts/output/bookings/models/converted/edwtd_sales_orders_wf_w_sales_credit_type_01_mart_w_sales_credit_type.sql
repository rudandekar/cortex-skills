{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_credit_type', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_CREDIT_TYPE',
        'target_table': 'W_SALES_CREDIT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.445361+00:00'
    }
) }}

WITH 

source_st_uo_oe_sales_credit_types AS (
    SELECT
        batch_id,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        context,
        description,
        enabled_flag,
        name,
        quota_flag,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        sales_credit_type_id,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_uo_oe_sales_credit_types') }}
),

transformed_exp_st_uo_oe_sales_credit_types AS (
    SELECT
    name,
    create_datetime,
    end_tv_date,
    description,
    enabled_flag,
    quota_flag,
    action_code,
    rank_index,
    dml_type
    FROM source_st_uo_oe_sales_credit_types
),

final AS (
    SELECT
        bk_sales_credit_type_code,
        start_tv_date,
        end_tv_date,
        sales_credit_type_description,
        sales_credit_type_enabled_flag,
        sales_credit_type_quota_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM transformed_exp_st_uo_oe_sales_credit_types
)

SELECT * FROM final