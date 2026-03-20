{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_overlay_data_mnl_incr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_OVERLAY_DATA_MNL_INCR',
        'target_table': 'WI_OVERLAY_DATA_MNL_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.515284+00:00'
    }
) }}

WITH 

source_wi_overlay_data_mnl_incr AS (
    SELECT
        sk_trx_sc_id_int,
        pd_ss_cd,
        sls_cr_assgn_manual_adj_key,
        bk_sls_terr_assignment_type_cd,
        scasa_creation_dt,
        sales_rep_num,
        sales_territory_key,
        manual_trx_key,
        sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_update_dtm,
        pd_sls_credit_last_update_dtm,
        sales_credit_usage_cd
    FROM {{ source('raw', 'wi_overlay_data_mnl_incr') }}
),

final AS (
    SELECT
        sk_trx_sc_id_int,
        pd_ss_cd,
        sls_cr_assgn_manual_adj_key,
        bk_sls_terr_assignment_type_cd,
        scasa_creation_dt,
        sales_rep_num,
        sales_territory_key,
        manual_trx_key,
        sales_commission_pct,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_update_dtm,
        pd_sls_credit_last_update_dtm,
        sales_credit_usage_cd
    FROM source_wi_overlay_data_mnl_incr
)

SELECT * FROM final