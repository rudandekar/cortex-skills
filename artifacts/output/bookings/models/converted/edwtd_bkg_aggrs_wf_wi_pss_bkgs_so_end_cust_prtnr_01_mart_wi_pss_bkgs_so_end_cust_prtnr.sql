{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pss_bkgs_so_end_cust_prtnr', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_PSS_BKGS_SO_END_CUST_PRTNR',
        'target_table': 'WI_PSS_BKGS_SO_END_CUST_PRTNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.592458+00:00'
    }
) }}

WITH 

source_wi_pss_bkgs_so_end_cust_prtnr AS (
    SELECT
        bkgs_measure_trans_type_cd,
        so_end_customer_key,
        so_partner_site_party_key,
        dv_source_order_num_int
    FROM {{ source('raw', 'wi_pss_bkgs_so_end_cust_prtnr') }}
),

final AS (
    SELECT
        bkgs_measure_trans_type_cd,
        so_end_customer_key,
        so_partner_site_party_key,
        dv_source_order_num_int
    FROM source_wi_pss_bkgs_so_end_cust_prtnr
)

SELECT * FROM final