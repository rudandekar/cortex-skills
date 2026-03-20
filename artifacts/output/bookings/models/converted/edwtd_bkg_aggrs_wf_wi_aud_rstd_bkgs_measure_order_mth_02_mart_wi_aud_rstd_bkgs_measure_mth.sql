{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_aud_rstd_bkgs_measure_order_mth', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_AUD_RSTD_BKGS_MEASURE_ORDER_MTH',
        'target_table': 'WI_AUD_RSTD_BKGS_MEASURE_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.792657+00:00'
    }
) }}

WITH 

source_wi_aud_rstd_bkg_thrshld_values AS (
    SELECT
        audit_table_name,
        audit_column_name,
        min_threshold_value,
        max_threshold_value,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'wi_aud_rstd_bkg_thrshld_values') }}
),

source_wi_aud_rstd_bkgs_measure_mth AS (
    SELECT
        audit_table_name,
        dv_fiscal_year_mth_number_int,
        fiscal_year_quarter_number_int,
        bkgs_measure_trans_type_code,
        trade_in_amount,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amount,
        dd_extended_hold_quantity,
        dd_comp_us_standard_price_amt,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'wi_aud_rstd_bkgs_measure_mth') }}
),

transformed_exptrans AS (
    SELECT
    audit_table_name,
    audit_column_name,
    amountdiff_flg,
    IFF(AMOUNTDIFF_FLG = 'Y' , ABORT('Amount not matching')) AS amountdiff_flg_out
    FROM source_wi_aud_rstd_bkgs_measure_mth
),

filtered_filtrans AS (
    SELECT *
    FROM transformed_exptrans
    WHERE FALSE
),

final AS (
    SELECT
        audit_table_name,
        dv_fiscal_year_mth_number_int,
        fiscal_year_quarter_number_int,
        bkgs_measure_trans_type_code,
        trade_in_amount,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amount,
        dd_extended_hold_quantity,
        dd_comp_us_standard_price_amt,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM filtered_filtrans
)

SELECT * FROM final