{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_credit_claim_pos_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_CREDIT_CLAIM_POS_TRX',
        'target_table': 'W_SALES_CREDIT_CLAIM_POS_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.533131+00:00'
    }
) }}

WITH 

source_st_otm_gct_pos_lines AS (
    SELECT
        pos_line_id,
        claim_id,
        pos_transaction_id,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_otm_gct_pos_lines') }}
),

transformed_exptrans AS (
    SELECT
    pos_line_id,
    claim_id,
    pos_transaction_id,
    batch_id,
    create_datetime,
    'RI' AS exception_type
    FROM source_st_otm_gct_pos_lines
),

transformed_exptrans1 AS (
    SELECT
    pos_transaction_id,
    claim_id,
    'I' AS action_code
    FROM transformed_exptrans
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
        bk_sales_credit_claim_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exptrans1
)

SELECT * FROM final