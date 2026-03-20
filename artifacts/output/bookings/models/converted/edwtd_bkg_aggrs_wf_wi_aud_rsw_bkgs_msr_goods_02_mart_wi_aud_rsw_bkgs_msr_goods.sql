{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_aud_rsw_bkgs_msr_goods', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_AUD_RSW_BKGS_MSR_GOODS',
        'target_table': 'WI_AUD_RSW_BKGS_MSR_GOODS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.398182+00:00'
    }
) }}

WITH 

source_wi_aud_rsw_bkgs_msr_thrhld_val AS (
    SELECT
        audit_table_name,
        audit_column_name,
        min_threshold_value,
        max_threshold_value,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'wi_aud_rsw_bkgs_msr_thrhld_val') }}
),

source_wi_aud_rsw_bkgs_msr_goods AS (
    SELECT
        audit_table_name,
        dv_fiscal_year_mth_number_int,
        bkgs_measure_trans_type_code,
        rs_dd_comp_us_net_price_amount,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'wi_aud_rsw_bkgs_msr_goods') }}
),

filtered_filtrans AS (
    SELECT *
    FROM source_wi_aud_rsw_bkgs_msr_goods
    WHERE FALSE
),

transformed_exptrans AS (
    SELECT
    audit_table_name,
    audit_column_name,
    amountdiff_flg,
    IFF(AMOUNTDIFF_FLG = 'Y' , ABORT('Amounts are not matching between RSTD_BKGS_MEASURE and RECURRING_SW_BKGS_MEASURE') ) AS amountdiff_flg_out
    FROM filtered_filtrans
),

final AS (
    SELECT
        audit_table_name,
        dv_fiscal_year_mth_number_int,
        bkgs_measure_trans_type_code,
        rs_dd_comp_us_net_price_amount,
        edw_create_user,
        edw_create_datetime
    FROM transformed_exptrans
)

SELECT * FROM final