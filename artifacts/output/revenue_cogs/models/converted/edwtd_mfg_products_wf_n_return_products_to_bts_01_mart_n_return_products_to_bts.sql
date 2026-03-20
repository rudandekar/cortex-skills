{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_return_products_to_bts', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RETURN_PRODUCTS_TO_BTS',
        'target_table': 'N_RETURN_PRODUCTS_TO_BTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.062520+00:00'
    }
) }}

WITH 

source_w_return_products_to_bts AS (
    SELECT
        return_products_to_bts_key,
        return_order_num,
        transmission_dt,
        process_status_cd,
        create_dtm,
        dv_create_dt,
        expected_arrival_dt,
        freight_carrier_name,
        sk_message_id,
        bts_partner_inventory_org_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_return_products_to_bts') }}
),

final AS (
    SELECT
        return_products_to_bts_key,
        return_order_num,
        transmission_dt,
        process_status_cd,
        create_dtm,
        dv_create_dt,
        expected_arrival_dt,
        freight_carrier_name,
        sk_message_id,
        bts_partner_inventory_org_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_return_products_to_bts
)

SELECT * FROM final