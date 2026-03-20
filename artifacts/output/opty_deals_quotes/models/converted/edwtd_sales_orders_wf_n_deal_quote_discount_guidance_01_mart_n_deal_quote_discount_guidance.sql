{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_quote_discount_guidance', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_QUOTE_DISCOUNT_GUIDANCE',
        'target_table': 'N_DEAL_QUOTE_DISCOUNT_GUIDANCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.941404+00:00'
    }
) }}

WITH 

source_w_deal_quote_discount_guidance AS (
    SELECT
        bk_quote_num,
        bk_guidance_id_int,
        ru_requested_by_user_id,
        is_finalized_flg,
        in_sales_dashboard_flg,
        requested_on_dt,
        requsted_by_user_role,
        sk_request_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_quote_discount_guidance') }}
),

final AS (
    SELECT
        bk_quote_num,
        bk_guidance_id_int,
        ru_requested_by_user_id,
        is_finalized_flg,
        in_sales_dashboard_flg,
        requested_on_dt,
        requsted_by_user_role,
        sk_request_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_deal_quote_discount_guidance
)

SELECT * FROM final