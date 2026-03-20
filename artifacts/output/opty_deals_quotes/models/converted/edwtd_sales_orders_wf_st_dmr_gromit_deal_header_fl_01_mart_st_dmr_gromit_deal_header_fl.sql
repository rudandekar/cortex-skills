{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dmr_gromit_deal_header_fl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_DMR_GROMIT_DEAL_HEADER_FL',
        'target_table': 'ST_DMR_GROMIT_DEAL_HEADER_FL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.970994+00:00'
    }
) }}

WITH 

source_ff_dmr_gromit_deal_header AS (
    SELECT
        deal_object_id,
        opty_number,
        deal_name,
        account_name,
        theater,
        sla_flag,
        standard_nonstandard,
        finance_controller,
        legal_assurer,
        opportunity_owner,
        deal_service_value,
        deal_status,
        market_segment,
        deal_last_update_date,
        deal_registration_date,
        approved_date_last,
        expiration_date,
        batch_id,
        create_datetime,
        action_cd
    FROM {{ source('raw', 'ff_dmr_gromit_deal_header') }}
),

final AS (
    SELECT
        batch_id,
        deal_object_id,
        opty_number,
        deal_name,
        account_name,
        theater,
        sla_flag,
        standard_nonstandard,
        finance_controller,
        legal_assurer,
        opportunity_owner,
        deal_service_value,
        deal_status,
        market_segment,
        deal_last_update_date,
        deal_registration_date,
        approved_date_last,
        expiration_date,
        create_datetime,
        action_cd
    FROM source_ff_dmr_gromit_deal_header
)

SELECT * FROM final