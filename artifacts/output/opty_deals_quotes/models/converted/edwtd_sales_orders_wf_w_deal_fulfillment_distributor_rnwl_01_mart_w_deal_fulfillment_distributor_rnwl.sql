{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_fulfillment_distributor_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_FULFILLMENT_DISTRIBUTOR_RNWL',
        'target_table': 'W_DEAL_FULFILLMENT_DISTRIBUTOR_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.954666+00:00'
    }
) }}

WITH 

source_w_deal_fulfillment_distributor_rnwl AS (
    SELECT
        deal_fulfillment_distri_key,
        sk_object_id_int,
        bk_deal_id,
        bk_quote_num,
        sk_distri_src_profile_id_int,
        distri_authorization_num,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_code,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_fulfillment_distributor_rnwl') }}
),

source_st_int_raw_deal_disti_fulfillment_rnwl AS (
    SELECT
        object_id,
        deal_object_id,
        authorization_number,
        deviation_id,
        deviation_status,
        dev_process_description,
        dev_process_remark,
        dev_process_status,
        dev_retry_count,
        end_date,
        last_update_date,
        linked_deviation_id,
        next_action,
        queue_object_id,
        quote_object_id,
        revision_number,
        source_profile_id,
        start_date
    FROM {{ source('raw', 'st_int_raw_deal_disti_fulfillment_rnwl') }}
),

final AS (
    SELECT
        deal_fulfillment_distri_key,
        sk_object_id_int,
        bk_deal_id,
        bk_quote_num,
        sk_distri_src_profile_id_int,
        distri_authorization_num,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_code,
        action_code,
        dml_type
    FROM source_st_int_raw_deal_disti_fulfillment_rnwl
)

SELECT * FROM final