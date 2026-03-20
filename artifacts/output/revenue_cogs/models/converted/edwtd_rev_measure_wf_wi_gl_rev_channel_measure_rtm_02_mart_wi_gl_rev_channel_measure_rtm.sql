{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rev_channel_measure_rtm', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REV_CHANNEL_MEASURE_RTM',
        'target_table': 'WI_GL_REV_CHANNEL_MEASURE_RTM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.187843+00:00'
    }
) }}

WITH 

source_wi_el_rtm_split_ratio AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        sub_measure_key,
        rtm_value,
        rtm_split_ratio
    FROM {{ source('raw', 'wi_el_rtm_split_ratio') }}
),

source_wi_gl_rev_channel_measure_rtm AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        partner_site_party_key,
        channel_flg,
        channel_drop_ship_flg,
        rtm_type,
        dv_route_to_market_cd,
        partner_type_cd,
        dv_pnl_line_item_name,
        action_code,
        dml_type,
        rtm_split_ratio
    FROM {{ source('raw', 'wi_gl_rev_channel_measure_rtm') }}
),

final AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        partner_site_party_key,
        channel_flg,
        channel_drop_ship_flg,
        rtm_type,
        dv_route_to_market_cd,
        partner_type_cd,
        dv_pnl_line_item_name,
        action_code,
        dml_type,
        rtm_split_ratio
    FROM source_wi_gl_rev_channel_measure_rtm
)

SELECT * FROM final