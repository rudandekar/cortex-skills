{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rev_chnl_msr_rtm_mth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REV_CHNL_MSR_RTM_MTH',
        'target_table': 'WI_GL_REV_CHNL_RTM_RTR_BCP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.566576+00:00'
    }
) }}

WITH 

source_wi_gl_rev_chnl_rtm_rtr_bcp AS (
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
        action_code,
        dml_type
    FROM {{ source('raw', 'wi_gl_rev_chnl_rtm_rtr_bcp') }}
),

source_wi_gl_rev_chnl_msr_rtm_mth AS (
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
        action_code,
        dml_type
    FROM {{ source('raw', 'wi_gl_rev_chnl_msr_rtm_mth') }}
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
        action_code,
        dml_type
    FROM source_wi_gl_rev_chnl_msr_rtm_mth
)

SELECT * FROM final