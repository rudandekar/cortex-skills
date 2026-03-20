{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_slc_site_cntnr_inv_outbound', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SLC_SITE_CNTNR_INV_OUTBOUND',
        'target_table': 'N_SLC_SITE_CNTNR_INV_OUTBOUND',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.554108+00:00'
    }
) }}

WITH 

source_w_slc_site_cntnr_inv_outbound AS (
    SELECT
        bk_strtgc_logistics_cntr_name,
        bk_shipset_num_int,
        sales_order_key,
        bk_carton_id,
        bk_requested_dtm,
        with_inbound_role,
        creation_dtm,
        ru_bk_inbound_response_dtm,
        dv_creation_dt,
        age_bucket_cnt,
        dv_requested_dt,
        sk_cycle_id_int_cnt,
        sk_message_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_slc_site_cntnr_inv_outbound') }}
),

final AS (
    SELECT
        bk_strtgc_logistics_cntr_name,
        bk_shipset_num_int,
        sales_order_key,
        bk_carton_id,
        bk_requested_dtm,
        with_inbound_role,
        creation_dtm,
        ru_bk_inbound_response_dtm,
        dv_creation_dt,
        age_bucket_cnt,
        dv_requested_dt,
        sk_cycle_id_int_cnt,
        sk_message_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_slc_site_cntnr_inv_outbound
)

SELECT * FROM final