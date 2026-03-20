{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_pss_bkgs_so_end_cust_prtnr', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_PSS_BKGS_SO_END_CUST_PRTNR',
        'target_table': 'MT_PSS_BKGS_SO_END_CUST_PRTNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.175159+00:00'
    }
) }}

WITH 

source_mt_pss_bkgs_so_end_cust_prtnr AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        so_end_customer_key,
        end_cust_branch_primary_name,
        so_partner_site_party_key,
        partner_primary_name,
        dv_source_order_num_int
    FROM {{ source('raw', 'mt_pss_bkgs_so_end_cust_prtnr') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        so_end_customer_key,
        end_cust_branch_primary_name,
        so_partner_site_party_key,
        partner_primary_name,
        dv_source_order_num_int
    FROM source_mt_pss_bkgs_so_end_cust_prtnr
)

SELECT * FROM final