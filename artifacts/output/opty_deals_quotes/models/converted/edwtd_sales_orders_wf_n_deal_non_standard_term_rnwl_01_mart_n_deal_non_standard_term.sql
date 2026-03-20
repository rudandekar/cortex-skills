{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_non_standard_term_rnwl', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_NON_STANDARD_TERM_RNWL',
        'target_table': 'N_DEAL_NON_STANDARD_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.988964+00:00'
    }
) }}

WITH 

source_w_deal_non_standard_term_rnwl AS (
    SELECT
        bk_non_standard_term_name,
        bk_deal_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        as_subscription_flg,
        as_transaction_flg,
        as_fixed_flg,
        tss_core_flg,
        ros_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_non_standard_term_rnwl') }}
),

final AS (
    SELECT
        bk_non_standard_term_name,
        bk_deal_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        as_subscription_flg,
        as_transaction_flg,
        as_fixed_flg,
        tss_core_flg,
        ros_flg
    FROM source_w_deal_non_standard_term_rnwl
)

SELECT * FROM final