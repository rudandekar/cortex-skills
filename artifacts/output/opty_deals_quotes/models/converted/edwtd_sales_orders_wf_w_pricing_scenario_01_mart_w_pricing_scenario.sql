{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_pricing_scenario', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_PRICING_SCENARIO',
        'target_table': 'W_PRICING_SCENARIO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.893907+00:00'
    }
) }}

WITH 

source_w_pricing_scenario AS (
    SELECT
        pricing_scenario_key,
        bk_deal_id,
        scenario_name,
        scenario_status_cd,
        ru_published_by_user_id,
        scenario_type_cd,
        cbn_status_cd,
        source_scenario_update_dtm,
        fullfillment_type_cd,
        is_finalized_flg,
        is_stringent_flg,
        active_flg,
        bk_price_list_name,
        cbn_partner_site_party_key,
        sk_scenario_id_int,
        ru_scenario_published_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_pricing_scenario') }}
),

final AS (
    SELECT
        pricing_scenario_key,
        bk_deal_id,
        scenario_name,
        scenario_status_cd,
        ru_published_by_user_id,
        scenario_type_cd,
        cbn_status_cd,
        source_scenario_update_dtm,
        fullfillment_type_cd,
        is_finalized_flg,
        is_stringent_flg,
        active_flg,
        bk_price_list_name,
        cbn_partner_site_party_key,
        sk_scenario_id_int,
        ru_scenario_published_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_pricing_scenario
)

SELECT * FROM final