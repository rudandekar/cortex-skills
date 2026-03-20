{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_cr_asgmt_nonapld ', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_SALES_CR_ASGMT_NONAPLD ',
        'target_table': 'N_SALES_CR_ASGMT_NONAPLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.276978+00:00'
    }
) }}

WITH 

source_w_sales_cr_asgmt_nonapld AS (
    SELECT
        sales_cr_asgmt_nonapld_key,
        ar_trx_line_key,
        start_tv_datetime,
        start_ssp_date,
        bk_sales_credit_type_code,
        bk_sales_rep_number,
        end_tv_datetime,
        end_ssp_date,
        sales_commission_percentage,
        bk_sales_territory_key,
        sk_line_sequence_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_cr_asgmt_nonapld') }}
),

final AS (
    SELECT
        ar_trx_line_key,
        sales_cr_asgmt_nonapld_key,
        bk_sales_credit_type_code,
        bk_sales_rep_number,
        sales_commission_percentage,
        bk_sales_territory_key,
        sk_line_sequence_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        sk_sc_agent_id_int
    FROM source_w_sales_cr_asgmt_nonapld
)

SELECT * FROM final