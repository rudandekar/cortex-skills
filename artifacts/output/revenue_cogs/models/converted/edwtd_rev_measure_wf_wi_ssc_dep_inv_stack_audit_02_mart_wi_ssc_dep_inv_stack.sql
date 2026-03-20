{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ssc_dep_inv_stack_audit', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SSC_DEP_INV_STACK_AUDIT',
        'target_table': 'WI_SSC_DEP_INV_STACK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.149578+00:00'
    }
) }}

WITH 

source_wi_ccm_fiscal_month_ctrl AS (
    SELECT
        fiscal_year_month_int,
        fiscal_year_number_int,
        fiscal_month_number_int,
        fiscal_month_name,
        fiscal_month_start_date,
        fiscal_month_end_date,
        fiscal_quarter_number_int,
        fiscal_quarter_name,
        fiscal_quarter_start_date,
        fiscal_quarter_end_date,
        fiscal_3year_ago_month_num_int,
        fiscal_month_ago_month_number,
        fiscal_month_id
    FROM {{ source('raw', 'wi_ccm_fiscal_month_ctrl') }}
),

source_wi_ssc_dep_inv_stack AS (
    SELECT
        product_family,
        fiscal_month_id,
        dep_inv_cost,
        quantity
    FROM {{ source('raw', 'wi_ssc_dep_inv_stack') }}
),

transformed_exptrans AS (
    SELECT
    start_date,
    end_date,
    fiscal_month_id,
    SETVARIABLE('START_DATE',START_DATE) AS o_start_date,
    SETVARIABLE('END_DATE',END_DATE) AS o_end_date,
    SETVARIABLE('FISCAL_MONTH_ID',FISCAL_MONTH_ID) AS o_fiscal_month_id
    FROM source_wi_ssc_dep_inv_stack
),

filtered_filtrans AS (
    SELECT *
    FROM transformed_exptrans
    WHERE 1=2
),

final AS (
    SELECT
        product_family,
        fiscal_month_id,
        dep_inv_cost,
        quantity
    FROM filtered_filtrans
)

SELECT * FROM final