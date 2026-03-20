{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rev_sca_nrt_smr_tv', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_REV_SCA_NRT_SMR_TV',
        'target_table': 'WI_REV_SCA_NRT_SMR_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.389289+00:00'
    }
) }}

WITH 

source_wi_rev_sca_nrt_smr_incr AS (
    SELECT
        sales_credit_assignment_key,
        sca_source_type_cd,
        sk_line_seq_id_int,
        ss_cd,
        ep_source_line_id_int,
        ep_sk_salesrep_id_int,
        sca_sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        sca_source_commit_dtm,
        edw_create_dtm,
        edw_update_dtm,
        ru_sales_order_line_key,
        ru_ar_transaction_line_key,
        ep_sk_sales_credit_type_id_int,
        ep_sk_territory_id_int,
        quota_flag,
        bk_sales_credit_type_code,
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        dv_fiscal_year_month_num_int,
        event_type,
        sales_rep_number,
        sales_territory_key,
        sk_sc_agent_id_int,
        sk_rtr_attribution_id,
        total_split_pct,
        sales_motion_cd,
        bk_offer_attribution_id_int,
        rtr_attribution_key,
        split_2_type_cd
    FROM {{ source('raw', 'wi_rev_sca_nrt_smr_incr') }}
),

final AS (
    SELECT
        sales_credit_assignment_key,
        sca_source_type_cd,
        sk_line_seq_id_int,
        ss_cd,
        ep_source_line_id_int,
        ep_sk_salesrep_id_int,
        sca_sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        sca_source_commit_dtm,
        edw_create_dtm,
        edw_update_dtm,
        ru_sales_order_line_key,
        ru_ar_transaction_line_key,
        ep_sk_sales_credit_type_id_int,
        ep_sk_territory_id_int,
        quota_flag,
        bk_sales_credit_type_code,
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        dv_fiscal_year_month_num_int,
        event_type,
        sales_rep_number,
        sales_territory_key,
        sk_sc_agent_id_int,
        sk_rtr_attribution_id,
        total_split_pct,
        sales_motion_cd,
        bk_offer_attribution_id_int,
        rtr_attribution_key,
        split_2_type_cd
    FROM source_wi_rev_sca_nrt_smr_incr
)

SELECT * FROM final