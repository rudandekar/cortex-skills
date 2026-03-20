{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_so_cr_asgn_for_appld_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SO_CR_ASGN_FOR_APPLD_TRX',
        'target_table': 'W_SO_CR_ASGN_FOR_APPLD_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.874112+00:00'
    }
) }}

WITH 

source_sm_so_cr_asgn_apld_trx AS (
    SELECT
        so_credit_asgn_applied_key,
        sk_header_sequence_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_so_cr_asgn_apld_trx') }}
),

source_wi_so_cr_asgn_for_appld_trx AS (
    SELECT
        source_deleted_flg,
        header_seq_id,
        global_name,
        salesrep_id,
        territory_id,
        sales_credit_type_id,
        source_header_id,
        split_percent,
        last_update_date,
        creation_date
    FROM {{ source('raw', 'wi_so_cr_asgn_for_appld_trx') }}
),

transformed_exptrans1 AS (
    SELECT
    source_deleted_flg,
    header_seq_id,
    global_name,
    salesrep_id,
    territory_id,
    sales_credit_type_id,
    source_header_id,
    split_percent,
    last_update_date,
    creation_date
    FROM source_wi_so_cr_asgn_for_appld_trx
),

transformed_exptrans AS (
    SELECT
    so_credit_asgn_applied_key,
    source_deleted_flg,
    split_percent,
    creation_date,
    last_update_date,
    source_header_id,
    salesrep_id,
    territory_id,
    sales_credit_type_id,
    header_seq_id,
    global_name,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    action_code,
    dml_type
    FROM transformed_exptrans1
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
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exptrans
)

SELECT * FROM final