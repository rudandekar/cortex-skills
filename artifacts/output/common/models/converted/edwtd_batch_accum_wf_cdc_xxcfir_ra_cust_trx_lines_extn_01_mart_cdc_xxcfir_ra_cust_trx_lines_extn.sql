{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cdc_xxcfir_ra_cust_trx_lines_extn', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CDC_XXCFIR_RA_CUST_TRX_LINES_EXTN',
        'target_table': 'CDC_XXCFIR_RA_CUST_TRX_LINES_EXTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.874781+00:00'
    }
) }}

WITH 

source_cdc_xxcfir_ra_cus_trx_lin_extn AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        customer_trx_line_id,
        customer_trx_id,
        unique_id,
        unit_list_price,
        oa_flag,
        isv_name,
        trueup_commitment_amount,
        trueup_usage_amount_for_period,
        concurrent_program_id,
        request_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        trial_credit_flag,
        ccw_order_line_id,
        ratio,
        subscription_number,
        amortized_schedule,
        amortized_subsc_credit_amount,
        bundle_ato_sku,
        brm_account,
        sku_purchase_time,
        subscription_credit_total,
        attribute4,
        attribute5,
        attribute3,
        commitment_based_usage,
        attribute8,
        attribute1,
        commitment_type,
        attribute9,
        attribute2,
        commitment_sku,
        attribute6,
        attribute10,
        attribute7,
        usage_gross_amount,
        commitment_amount,
        subscription_scale
    FROM {{ source('raw', 'cdc_xxcfir_ra_cus_trx_lin_extn') }}
),

source_stg_cdc_xxcfir_ra_cust_trx_lines_extn AS (
    SELECT
        customer_trx_line_id,
        customer_trx_id,
        unique_id,
        unit_list_price,
        oa_flag,
        isv_name,
        trueup_commitment_amount,
        trueup_usage_amount_for_period,
        concurrent_program_id,
        request_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        trial_credit_flag,
        subscription_number,
        ratio,
        ccw_order_line_id
    FROM {{ source('raw', 'stg_cdc_xxcfir_ra_cust_trx_lines_extn') }}
),

transformed_exp_cdc_xxcfir_ra_cust_trx_lines_extn AS (
    SELECT
    customer_trx_line_id,
    customer_trx_id,
    unique_id,
    unit_list_price,
    oa_flag,
    isv_name,
    trueup_commitment_amount,
    trueup_usage_amount_for_period,
    concurrent_program_id,
    request_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    trial_credit_flag,
    ccw_order_line_id,
    ratio,
    subscription_number,
    refresh_datetime,
    source_commit_time,
    source_dml_type
    FROM source_stg_cdc_xxcfir_ra_cust_trx_lines_extn
),

final AS (
    SELECT
        customer_trx_line_id,
        customer_trx_id,
        unique_id,
        unit_list_price,
        oa_flag,
        isv_name,
        trueup_commitment_amount,
        trueup_usage_amount_for_period,
        concurrent_program_id,
        request_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        trial_credit_flag,
        subscription_number,
        ratio,
        ccw_order_line_id
    FROM transformed_exp_cdc_xxcfir_ra_cust_trx_lines_extn
)

SELECT * FROM final