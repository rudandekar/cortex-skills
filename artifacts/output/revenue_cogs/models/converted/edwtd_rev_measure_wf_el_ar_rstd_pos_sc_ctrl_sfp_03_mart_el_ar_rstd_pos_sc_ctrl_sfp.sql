{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ar_rstd_pos_sc_ctrl_sfp', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_AR_RSTD_POS_SC_CTRL_SFP',
        'target_table': 'EL_AR_RSTD_POS_SC_CTRL_SFP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.437800+00:00'
    }
) }}

WITH 

source_el_pos_transaction_line_sfp AS (
    SELECT
        bk_pos_transaction_id_int,
        cisco_internal_pos_flg,
        sales_order_line_key,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'el_pos_transaction_line_sfp') }}
),

source_el_ar_rstd_pos_sc_ctrl_sfp AS (
    SELECT
        fiscal_month_id,
        start_date,
        end_date,
        fiscal_week_id,
        dv_fiscal_year_age,
        sfp_incr_fiscal_month_id,
        min_gl_revenue_measure_key,
        min_inv_revenue_measure_key,
        edw_update_dtm
    FROM {{ source('raw', 'el_ar_rstd_pos_sc_ctrl_sfp') }}
),

source_el_pos_sca_adj_cng_drct_sfp AS (
    SELECT
        pd_bk_pos_transaction_id_int,
        pd_sales_rep_num,
        pd_sales_territory_key,
        pd_sales_commission_pct,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'el_pos_sca_adj_cng_drct_sfp') }}
),

final AS (
    SELECT
        fiscal_month_id,
        start_date,
        end_date,
        fiscal_week_id,
        dv_fiscal_year_age,
        sfp_incr_fiscal_month_id,
        min_gl_revenue_measure_key,
        min_inv_revenue_measure_key,
        edw_update_dtm
    FROM source_el_pos_sca_adj_cng_drct_sfp
)

SELECT * FROM final