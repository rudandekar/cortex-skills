{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_fulfillment_distributor', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_FULFILLMENT_DISTRIBUTOR',
        'target_table': 'N_DEAL_FULFILLMENT_DISTRIBUTOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.952842+00:00'
    }
) }}

WITH 

source_w_deal_fulfillment_distributor AS (
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
    FROM {{ source('raw', 'w_deal_fulfillment_distributor') }}
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
        ss_code
    FROM source_w_deal_fulfillment_distributor
)

SELECT * FROM final