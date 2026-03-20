{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ssc_activity_cost', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SSC_ACTIVITY_COST',
        'target_table': 'WI_TPL_COST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.599360+00:00'
    }
) }}

WITH 

source_wi_ssc_dept_cost AS (
    SELECT
        ship_fiscal_month_id,
        ship_to_theater,
        ship_to_ctry_code,
        report_type,
        freight_type,
        product_family,
        prod_part_number,
        pid_cost,
        vol_cost,
        pid_count,
        vol_multiplier,
        theater_er_cost,
        theater_multiplier
    FROM {{ source('raw', 'wi_ssc_dept_cost') }}
),

source_wi_rma_sr_gtemp AS (
    SELECT
        sr_number,
        order_sa_number
    FROM {{ source('raw', 'wi_rma_sr_gtemp') }}
),

source_wi_ssc_net_repair_cost AS (
    SELECT
        ship_fiscal_month_id,
        ship_to_theater,
        ship_to_ctry_code,
        report_type,
        freight_type,
        product_family,
        prod_part_number,
        pid_cost,
        vol_cost,
        pid_count,
        vol_multiplier,
        theater_er_cost,
        theater_multiplier
    FROM {{ source('raw', 'wi_ssc_net_repair_cost') }}
),

source_wi_ssc_depr_cost AS (
    SELECT
        ship_fiscal_month_id,
        ship_to_theater,
        ship_to_ctry_code,
        report_type,
        freight_type,
        product_family,
        prod_part_number,
        pid_cost,
        vol_cost,
        pid_count,
        vol_multiplier,
        theater_er_cost,
        theater_multiplier,
        new_depr_by_theater,
        new_depr_cost
    FROM {{ source('raw', 'wi_ssc_depr_cost') }}
),

source_wi_tpl_cost AS (
    SELECT
        ship_fiscal_month_id,
        ship_to_theater,
        ship_to_ctry_code,
        report_type,
        freight_type,
        product_family,
        prod_part_number,
        pid_cost,
        vol_cost,
        pid_count,
        vol_multiplier,
        theater_er_cost,
        theater_multiplier
    FROM {{ source('raw', 'wi_tpl_cost') }}
),

source_wi_ssc_activity_cost_fin AS (
    SELECT
        sr_number,
        order_number,
        product_family,
        fiscal_ship_quarter,
        extended_standard_cost,
        site_country,
        site_theater,
        order_sa_number,
        order_sa_service_line,
        product_part_number,
        contracted_service_group,
        quantity_shipped,
        requested_service_group,
        extended_list_price,
        ship_fiscal_month,
        sr_type,
        fiscal_month_id,
        lookup_theater,
        dept_cost,
        depreciation_cost,
        repair_cost,
        duty_vat_cost,
        tpl_cost,
        tpm_cost,
        oem_cost,
        delivery_channel,
        technology,
        complexity,
        customer,
        contract,
        hw_family,
        market_segment,
        acod,
        close_date,
        tac_activity_dc,
        fin_sr_number,
        fin_theater,
        fin_dc,
        tac_act_theater,
        tac_act_dc,
        nam_technology,
        former_technology,
        created_date,
        tspc_id,
        curr_complexity,
        curr_customer,
        curr_contract,
        curr_hw_family,
        curr_market_segment,
        curr_acod,
        curr_nam_technology,
        curr_former_technology,
        sa_number,
        curr_sa_number,
        failure_code,
        warranty_parts_pct
    FROM {{ source('raw', 'wi_ssc_activity_cost_fin') }}
),

source_wi_ssc_unit_cost_fin AS (
    SELECT
        ship_fiscal_month_id,
        ship_to_theater,
        ship_to_ctry_code,
        report_type,
        freight_type,
        product_family,
        prod_part_number,
        pid_cost,
        vol_cost,
        pid_count,
        vol_multiplier,
        theater_er_cost,
        theater_multiplier
    FROM {{ source('raw', 'wi_ssc_unit_cost_fin') }}
),

source_wi_duty_vat_cost AS (
    SELECT
        ship_fiscal_month_id,
        ship_to_theater,
        ship_to_ctry_code,
        report_type,
        freight_type,
        product_family,
        prod_part_number,
        numr,
        pid_cost,
        vol_cost,
        pid_count,
        vol_multiplier,
        theater_er_cost,
        theater_multiplier
    FROM {{ source('raw', 'wi_duty_vat_cost') }}
),

source_wi_ssc_tpm_cost AS (
    SELECT
        ship_fiscal_month_id,
        ship_to_theater,
        ship_to_ctry_code,
        report_type,
        freight_type,
        product_family,
        prod_part_number,
        pid_cost,
        vol_cost,
        pid_count,
        vol_multiplier,
        theater_er_cost,
        theater_multiplier
    FROM {{ source('raw', 'wi_ssc_tpm_cost') }}
),

source_wi_ssc_oem_cost AS (
    SELECT
        ship_fiscal_month_id,
        ship_to_theater,
        ship_to_ctry_code,
        report_type,
        freight_type,
        product_family,
        prod_part_number,
        pid_cost,
        vol_cost,
        pid_count,
        vol_multiplier,
        theater_er_cost,
        theater_multiplier
    FROM {{ source('raw', 'wi_ssc_oem_cost') }}
),

source_wi_sr_fact AS (
    SELECT
        sr_number,
        theater_name,
        delivery_channel,
        complexity_id,
        customer,
        contract,
        hw_family,
        market_segment,
        close_date,
        sa_number,
        updated_cot_tech_key,
        technology,
        former_technology,
        acod
    FROM {{ source('raw', 'wi_sr_fact') }}
),

final AS (
    SELECT
        ship_fiscal_month_id,
        ship_to_theater,
        ship_to_ctry_code,
        report_type,
        freight_type,
        product_family,
        prod_part_number,
        pid_cost,
        vol_cost,
        pid_count,
        vol_multiplier,
        theater_er_cost,
        theater_multiplier
    FROM source_wi_sr_fact
)

SELECT * FROM final