{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_adj_disc_deal_type', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_ADJ_DISC_DEAL_TYPE',
        'target_table': 'WI_ADJ_DISC_DEAL_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.764932+00:00'
    }
) }}

WITH 

source_wi_adj_disc_deal_type AS (
    SELECT
        bk_quote_num,
        deal_id,
        bom_source_cd,
        std_incr_flg,
        source_reported_deal_type_cd,
        deal_subtype,
        gdr_flg,
        bk_promotion_cd,
        channel_program_name,
        promotion_name,
        rnsd_flg,
        non_standard_discount_usd_amt,
        transcation_rnsd_flg,
        source_mdm_flg,
        non_std_disc_flg,
        non_std_con_flg,
        deal_type
    FROM {{ source('raw', 'wi_adj_disc_deal_type') }}
),

final AS (
    SELECT
        bk_quote_num,
        deal_id,
        bom_source_cd,
        std_incr_flg,
        source_reported_deal_type_cd,
        deal_subtype,
        gdr_flg,
        bk_promotion_cd,
        channel_program_name,
        promotion_name,
        rnsd_flg,
        non_standard_discount_usd_amt,
        transcation_rnsd_flg,
        source_mdm_flg,
        non_std_disc_flg,
        non_std_con_flg,
        deal_type
    FROM source_wi_adj_disc_deal_type
)

SELECT * FROM final