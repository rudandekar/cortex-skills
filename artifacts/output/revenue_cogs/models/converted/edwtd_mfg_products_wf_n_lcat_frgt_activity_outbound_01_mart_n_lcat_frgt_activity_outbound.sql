{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_lcat_frgt_activity_outbound', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_LCAT_FRGT_ACTIVITY_OUTBOUND',
        'target_table': 'N_LCAT_FRGT_ACTIVITY_OUTBOUND',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.665079+00:00'
    }
) }}

WITH 

source_w_lcat_frgt_activity_outbound AS (
    SELECT
        lcat_frgt_actvty_outbound_key,
        freight_paid_usd_amt,
        frgt_rt_based_charge_usd_amt,
        freight_billed_usd_amt,
        shipment_leg_type_cd,
        destination_city_name,
        actual_shipset_weight_kg_amt,
        dimensional_weight_kg_amt,
        sk_activity_id_int,
        bk_ship_set_num_int,
        sales_order_key,
        ship_from_inv_org_key,
        ship_to_country_cd,
        freight_carrier_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type,
        gl_freight_usd_cost,
        gl_freight_type_cd,
        freight_planned,
        attribute15
    FROM {{ source('raw', 'w_lcat_frgt_activity_outbound') }}
),

final AS (
    SELECT
        lcat_frgt_actvty_outbound_key,
        freight_paid_usd_amt,
        frgt_rt_based_charge_usd_amt,
        freight_billed_usd_amt,
        shipment_leg_type_cd,
        destination_city_name,
        actual_shipset_weight_kg_amt,
        dimensional_weight_kg_amt,
        sk_activity_id_int,
        bk_ship_set_num_int,
        sales_order_key,
        ship_from_inv_org_key,
        ship_to_country_cd,
        freight_carrier_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        gl_freight_usd_cost,
        gl_freight_type_cd,
        original_source_sjprod_flg,
        freight_planned_usd_cost
    FROM source_w_lcat_frgt_activity_outbound
)

SELECT * FROM final