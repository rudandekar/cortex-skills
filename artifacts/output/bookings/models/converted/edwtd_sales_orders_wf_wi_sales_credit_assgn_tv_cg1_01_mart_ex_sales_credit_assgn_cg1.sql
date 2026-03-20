{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_credit_assgn_tv_cg1', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CREDIT_ASSGN_TV_CG1',
        'target_table': 'EX_SALES_CREDIT_ASSGN_CG1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.430132+00:00'
    }
) }}

WITH 

source_ex_sales_credit_assgn_cg1 AS (
    SELECT
        sales_credit_assignment_key,
        ep_source_line_id_int,
        sca_transaction_type,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sk_line_seq_id_int,
        ss_cd,
        sca_source_type_cd,
        sca_sales_commission_pct,
        bk_sales_credit_type_code,
        ep_sk_sales_credit_type_id_int,
        sales_rep_number,
        ep_sk_salesrep_id_int,
        sales_territory_key,
        ep_sk_territory_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_ar_transaction_line_key,
        ru_sales_order_line_key,
        start_tv_dtm,
        end_tv_dtm,
        exception_type,
        sk_sc_agent_id_int
    FROM {{ source('raw', 'ex_sales_credit_assgn_cg1') }}
),

source_wi_sales_credit_assgn_tv_cg1 AS (
    SELECT
        sales_credit_asgn_key,
        sales_order_line_key,
        bk_sales_rep_number,
        bk_sales_credit_type_code,
        start_tv_datetime,
        start_ssp_date,
        end_tv_datetime,
        end_ssp_date,
        sales_commission_percentage,
        sca_source_type_code,
        sales_territory_key,
        ss_code,
        sk_line_seq_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        sk_sc_agent_id_int
    FROM {{ source('raw', 'wi_sales_credit_assgn_tv_cg1') }}
),

final AS (
    SELECT
        sales_credit_assignment_key,
        ep_source_line_id_int,
        sca_transaction_type,
        sca_source_commit_dtm,
        dv_sca_source_commit_dt,
        sca_source_update_dtm,
        dv_sca_source_update_dt,
        sk_line_seq_id_int,
        ss_cd,
        sca_source_type_cd,
        sca_sales_commission_pct,
        bk_sales_credit_type_code,
        ep_sk_sales_credit_type_id_int,
        sales_rep_number,
        ep_sk_salesrep_id_int,
        sales_territory_key,
        ep_sk_territory_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_ar_transaction_line_key,
        ru_sales_order_line_key,
        start_tv_dtm,
        end_tv_dtm,
        exception_type,
        sk_sc_agent_id_int
    FROM source_wi_sales_credit_assgn_tv_cg1
)

SELECT * FROM final