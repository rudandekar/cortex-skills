{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_subscription_line_tsv', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_W_SUBSCRIPTION_LINE_TSV',
        'target_table': 'SM_SUBSCRIPTION_LINE_TSV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.695163+00:00'
    }
) }}

WITH 

source_ex_cisco_subscr_tsv_data_v AS (
    SELECT
        subscription_ref_id,
        subscription_id,
        status,
        initial_term,
        renewal_term,
        prepayment_term,
        renewal_counter,
        bdom,
        term_start_date,
        term_end_date,
        offer_type,
        billing_model,
        line_status,
        last_order_type,
        currency_code,
        product_id,
        tsv_amount,
        creation_date,
        last_modified_date,
        cancel_date,
        charge_type,
        offer_code,
        exception_type,
        flexup_sku
    FROM {{ source('raw', 'ex_cisco_subscr_tsv_data_v') }}
),

source_w_subscription_line_tsv AS (
    SELECT
        subscription_line_tsv_key,
        subscription_ref_id,
        bk_exaas_subscr_num,
        product_key,
        subscription_status_cd,
        initial_term_mths_cnt,
        renewal_term_mths_cnt,
        prepaid_term_mths_cnt,
        renewal_cnt,
        billed_day_of_the_mth_int,
        term_start_dtm,
        term_end_dtm,
        offer_type_cd,
        billing_model_name,
        subscr_line_status_cd,
        sales_order_type_cd,
        bk_iso_currency_cd,
        tsv_local_amt,
        src_creation_dtm,
        src_last_updated_dtm,
        src_cancel_dtm,
        charge_type_cd,
        ato_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        flex_up_resv_line_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_subscription_line_tsv') }}
),

source_sm_subscription_line_tsv AS (
    SELECT
        subscription_line_tsv_key,
        subscription_ref_id,
        bk_exaas_subscr_num,
        product_key,
        edw_create_dtm,
        edw_create_user,
        flex_up_resv_line_flg
    FROM {{ source('raw', 'sm_subscription_line_tsv') }}
),

final AS (
    SELECT
        subscription_line_tsv_key,
        subscription_ref_id,
        bk_exaas_subscr_num,
        product_key,
        edw_create_dtm,
        edw_create_user,
        flex_up_resv_line_flg
    FROM source_sm_subscription_line_tsv
)

SELECT * FROM final