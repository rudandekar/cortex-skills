{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_indrct_cogs_adj_alloc_tv', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_N_INDRCT_COGS_ADJ_ALLOC_TV',
        'target_table': 'N_INDRCT_COGS_ADJ_ALLOC_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.070981+00:00'
    }
) }}

WITH 

source_w_indrct_cogs_adj_alloc AS (
    SELECT
        indirect_adjustment_key,
        start_ssp_dt,
        end_ssp_dt,
        cogs_allocation_usd_amt,
        fiscal_year_number_int,
        fiscal_month_number_int,
        fiscal_calendar_cd,
        adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        dd_process_dt,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_indrct_cogs_adj_alloc') }}
),

final AS (
    SELECT
        indirect_adjustment_key,
        start_ssp_dt,
        end_ssp_dt,
        cogs_allocation_usd_amt,
        fiscal_year_number_int,
        fiscal_month_number_int,
        fiscal_calendar_cd,
        adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        dd_process_dt,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        bk_deal_id
    FROM source_w_indrct_cogs_adj_alloc
)

SELECT * FROM final