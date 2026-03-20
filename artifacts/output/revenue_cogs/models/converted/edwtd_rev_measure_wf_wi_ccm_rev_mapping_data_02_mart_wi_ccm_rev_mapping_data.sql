{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_rev_mapping_data', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_REV_MAPPING_DATA',
        'target_table': 'WI_CCM_REV_MAPPING_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.860131+00:00'
    }
) }}

WITH 

source_wi_ccm_rev_mapping_data AS (
    SELECT
        fiscal_quarter_name,
        service_contract_note_txt,
        drvd_sales_territory_key,
        dv_comp_us_net_rev_amt,
        total_comp_us_net_rev_amt,
        slk_dist_count,
        sa_slk_allocation_ratio
    FROM {{ source('raw', 'wi_ccm_rev_mapping_data') }}
),

source_wi_ccm_rev_mapng_data_srvc_cnt AS (
    SELECT
        service_contract_note_txt
    FROM {{ source('raw', 'wi_ccm_rev_mapng_data_srvc_cnt') }}
),

source_wi_ccm_rev_mapping_data AS (
    SELECT
        fiscal_quarter_name,
        service_contract_note_txt,
        drvd_sales_territory_key,
        dv_comp_us_net_rev_amt,
        total_comp_us_net_rev_amt,
        slk_dist_count,
        sa_slk_allocation_ratio
    FROM {{ source('raw', 'wi_ccm_rev_mapping_data') }}
),

final AS (
    SELECT
        fiscal_quarter_name,
        service_contract_note_txt,
        drvd_sales_territory_key,
        dv_comp_us_net_rev_amt,
        total_comp_us_net_rev_amt,
        slk_dist_count,
        sa_slk_allocation_ratio
    FROM source_wi_ccm_rev_mapping_data
)

SELECT * FROM final