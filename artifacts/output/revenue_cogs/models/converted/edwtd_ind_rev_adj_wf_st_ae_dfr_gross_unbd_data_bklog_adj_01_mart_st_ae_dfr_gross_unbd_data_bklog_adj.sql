{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_dfr_gross_unbd_data_bklog_adj', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_ST_AE_DFR_GROSS_UNBD_DATA_BKLOG_ADJ',
        'target_table': 'ST_AE_DFR_GROSS_UNBD_DATA_BKLOG_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.353593+00:00'
    }
) }}

WITH 

source_ff_dfr_gross_unbd_data_bklog_adj AS (
    SELECT
        processed_fiscal_month_id,
        source_entity,
        measure_name,
        service_flg,
        sales_territory_code,
        product_id,
        deal_id,
        contract_id,
        contract_start_date,
        contract_end_date,
        contract_term,
        gross_unbilled_deferred_rev,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        rpo_flg,
        fiscal_month_id,
        rev_type
    FROM {{ source('raw', 'ff_dfr_gross_unbd_data_bklog_adj') }}
),

final AS (
    SELECT
        processed_fiscal_month_id,
        source_entity,
        measure_name,
        service_flg,
        sales_territory_code,
        product_id,
        deal_id,
        contract_id,
        contract_start_date,
        contract_end_date,
        contract_term,
        gross_unbilled_deferred_rev,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        rpo_flag,
        fiscal_month_id,
        rev_type
    FROM source_ff_dfr_gross_unbd_data_bklog_adj
)

SELECT * FROM final