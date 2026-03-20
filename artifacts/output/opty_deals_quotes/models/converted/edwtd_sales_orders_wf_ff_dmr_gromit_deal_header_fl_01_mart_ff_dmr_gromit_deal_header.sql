{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_dmr_gromit_deal_header_fl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_FF_DMR_GROMIT_DEAL_HEADER_FL',
        'target_table': 'FF_DMR_GROMIT_DEAL_HEADER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.938785+00:00'
    }
) }}

WITH 

source_dmr_gromit_deal_header AS (
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
        expiration_date
    FROM {{ source('raw', 'dmr_gromit_deal_header') }}
),

transformed_exp_src_to_ff AS (
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
    'Batch_Id' AS out_batch_id,
    CURRENT_TIMESTAMP() AS out_create_datetime,
    'I' AS out_action_cd
    FROM source_dmr_gromit_deal_header
),

final AS (
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
    FROM transformed_exp_src_to_ff
)

SELECT * FROM final