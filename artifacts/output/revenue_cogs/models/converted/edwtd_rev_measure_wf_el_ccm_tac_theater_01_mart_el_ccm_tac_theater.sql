{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccm_tac_theater', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_CCM_TAC_THEATER',
        'target_table': 'EL_CCM_TAC_THEATER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.247522+00:00'
    }
) }}

WITH 

source_twfm003_theater AS (
    SELECT
        name,
        description,
        display_value,
        fin_transit_theater,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        technology_map,
        hc_flag,
        hc_mos_tech_adj,
        hc_mos_tech_round,
        hc_mos_spec_adj,
        hc_mos_spec_round,
        hc_mos_exp_adj,
        hc_mos_exp_round,
        ssc_flag,
        carma_cust_theater,
        ext_fin_cust_theater,
        fin_flag,
        route_center
    FROM {{ source('raw', 'twfm003_theater') }}
),

final AS (
    SELECT
        name,
        description,
        display_value,
        fin_transit_theater,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        technology_map,
        hc_flag,
        hc_mos_tech_adj,
        hc_mos_tech_round,
        hc_mos_spec_adj,
        hc_mos_spec_round,
        hc_mos_exp_adj,
        hc_mos_exp_round,
        ssc_flag,
        carma_cust_theater,
        ext_fin_cust_theater,
        fin_flag,
        route_center
    FROM source_twfm003_theater
)

SELECT * FROM final