{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_nr_tms_qtr_age', 'batch', 'edwtd_mkt_cic'],
    meta={
        'source_workflow': 'wf_m_WI_NR_TMS_QTR_AGE',
        'target_table': 'WI_NR_TMS_QTR_AGE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.083629+00:00'
    }
) }}

WITH 

source_wi_nr_tms_merge AS (
    SELECT
        advanced_technology_name,
        dd_customer_party_key,
        hq_customer_party_key,
        fiscal_year_quarter_number_int,
        new_repeat_flg,
        last_purchase_flg,
        new_repeat_tms_flg,
        last_purchase_tms_flg,
        new_repeat_hq_flg,
        last_purchase_hq_flg,
        new_repeat_tms_hq_flg,
        last_purchase_tms_hq_flg
    FROM {{ source('raw', 'wi_nr_tms_merge') }}
),

final AS (
    SELECT
        advanced_technology_name,
        dd_customer_party_key,
        hq_customer_party_key,
        fiscal_year_quarter_number_int,
        fiscal_qtr_age
    FROM source_wi_nr_tms_merge
)

SELECT * FROM final