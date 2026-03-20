{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_overlay_data_pos_incr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_OVERLAY_DATA_POS_INCR',
        'target_table': 'WI_OVERLAY_DATA_POS_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.388166+00:00'
    }
) }}

WITH 

source_wi_overlay_data_pos_incr AS (
    SELECT
        sk_trx_sc_id_int,
        pd_ss_cd,
        pos_scaac_key,
        pd_bk_sales_credit_type_cd,
        bk_sls_terr_assignment_type_cd,
        pd_transaction_dt,
        pd_bk_sales_rep_num,
        pd_sales_territory_key,
        pd_bk_pos_transaction_id_int,
        pd_sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_update_dtm,
        pd_sls_credit_last_update_dtm,
        pd_sales_credit_usage_cd,
        sk_offer_attribution_id_int,
        pos_trx_line_posted_date
    FROM {{ source('raw', 'wi_overlay_data_pos_incr') }}
),

final AS (
    SELECT
        sk_trx_sc_id_int,
        pd_ss_cd,
        pos_scaac_key,
        pd_bk_sales_credit_type_cd,
        bk_sls_terr_assignment_type_cd,
        pd_transaction_dt,
        pd_bk_sales_rep_num,
        pd_sales_territory_key,
        pd_bk_pos_transaction_id_int,
        pd_sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_update_dtm,
        pd_sls_credit_last_update_dtm,
        pd_sales_credit_usage_cd,
        sk_offer_attribution_id_int,
        pos_trx_line_posted_date
    FROM source_wi_overlay_data_pos_incr
)

SELECT * FROM final