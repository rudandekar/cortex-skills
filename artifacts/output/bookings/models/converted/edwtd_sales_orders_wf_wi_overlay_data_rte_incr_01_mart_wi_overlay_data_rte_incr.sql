{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_overlay_data_rte_incr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_OVERLAY_DATA_RTE_INCR',
        'target_table': 'WI_OVERLAY_DATA_RTE_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.002907+00:00'
    }
) }}

WITH 

source_wi_overlay_data_rte_incr AS (
    SELECT
        sk_trx_sc_id_int,
        pd_ss_cd,
        sales_credit_assignment_key,
        bk_sls_terr_assignment_type_cd,
        pd_scaa_creation_dt,
        pd_sales_rep_num,
        pd_sales_territory_key,
        ep_transaction_id_int,
        pd_sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_update_dtm,
        pd_sls_credit_last_update_dtm,
        sales_credit_type_cd,
        bk_offer_attribution_id_int,
        data_indicator,
        line_creation_date
    FROM {{ source('raw', 'wi_overlay_data_rte_incr') }}
),

final AS (
    SELECT
        sk_trx_sc_id_int,
        pd_ss_cd,
        sales_credit_assignment_key,
        bk_sls_terr_assignment_type_cd,
        pd_scaa_creation_dt,
        pd_sales_rep_num,
        pd_sales_territory_key,
        ep_transaction_id_int,
        pd_sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_update_dtm,
        pd_sls_credit_last_update_dtm,
        sales_credit_type_cd,
        bk_offer_attribution_id_int,
        data_indicator,
        line_creation_date
    FROM source_wi_overlay_data_rte_incr
)

SELECT * FROM final