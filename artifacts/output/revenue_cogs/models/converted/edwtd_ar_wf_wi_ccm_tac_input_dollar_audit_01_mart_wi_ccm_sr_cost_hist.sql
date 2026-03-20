{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_tac_input_dollar', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_TAC_INPUT_DOLLAR',
        'target_table': 'WI_CCM_SR_COST_HIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.093803+00:00'
    }
) }}

WITH 

source_wi_ccm_sr_cost_hist AS (
    SELECT
        fiscal_year_month_int,
        sr_number,
        transit_theater,
        delivery_channel,
        wg_name,
        acod_accrued,
        tac_cost,
        tsb_cost,
        tbo_cost,
        init_severity,
        updated_cot_tech_key,
        problem_code,
        accrued_hc_acod,
        accrued_fin_acod,
        acod,
        complexity,
        customer,
        service_sub_group,
        hw_family,
        ent_product_id,
        market_segment,
        cust_country,
        sa_number,
        cec_id,
        entry_channel,
        closed_delivery_channel,
        sr_type,
        contract,
        technology,
        global_technology,
        case_weight,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'wi_ccm_sr_cost_hist') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        sr_number,
        transit_theater,
        delivery_channel,
        wg_name,
        acod_accrued,
        tac_cost,
        tsb_cost,
        tbo_cost,
        init_severity,
        updated_cot_tech_key,
        problem_code,
        accrued_hc_acod,
        accrued_fin_acod,
        acod,
        complexity,
        customer,
        service_sub_group,
        hw_family,
        ent_product_id,
        market_segment,
        cust_country,
        sa_number,
        cec_id,
        entry_channel,
        closed_delivery_channel,
        sr_type,
        contract,
        technology,
        global_technology,
        case_weight,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM source_wi_ccm_sr_cost_hist
)

SELECT * FROM final