{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_so_cr_asgn_for_appld_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SO_CR_ASGN_FOR_APPLD_TRX',
        'target_table': 'N_SO_CR_ASGN_FOR_APPLD_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.084878+00:00'
    }
) }}

WITH 

source_w_so_cr_asgn_for_appld_trx AS (
    SELECT
        so_credit_asgn_applied_key,
        source_deleted_flg,
        sales_commission_pct,
        source_last_update_dtm,
        source_create_dtm,
        sales_order_key,
        sales_rep_num,
        sales_territory_key,
        sales_credit_type_cd,
        sk_header_sequence_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_so_cr_asgn_for_appld_trx') }}
),

final AS (
    SELECT
        so_credit_asgn_applied_key,
        source_deleted_flg,
        sales_commission_pct,
        source_last_update_dtm,
        source_create_dtm,
        sales_order_key,
        sales_rep_num,
        sales_territory_key,
        sales_credit_type_cd,
        sk_header_sequence_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_so_cr_asgn_for_appld_trx
)

SELECT * FROM final