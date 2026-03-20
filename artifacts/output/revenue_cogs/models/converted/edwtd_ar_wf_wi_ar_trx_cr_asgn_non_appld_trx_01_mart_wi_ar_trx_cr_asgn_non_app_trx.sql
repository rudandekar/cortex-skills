{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ar_trx_cr_asgn_non_appld_trx', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_AR_TRX_CR_ASGN_NON_APPLD_TRX',
        'target_table': 'WI_AR_TRX_CR_ASGN_NON_APP_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.138646+00:00'
    }
) }}

WITH 

source_wi_ar_trx_cr_asgn_non_app_trx AS (
    SELECT
        source_deleted_flg,
        assignment_mode,
        copy_from_minx_order_number,
        copy_from_source_header_id,
        copy_from_source_type,
        created_by,
        creation_date,
        deal_scmt_id,
        ges_update_date,
        global_name,
        header_seq_id,
        interfaced_to_cdw_flag,
        last_updated_by,
        last_update_date,
        last_update_login,
        minx_order_number,
        salesrep_id,
        sales_credit_type_id,
        source_header_id,
        source_type,
        split_percent,
        territory_id
    FROM {{ source('raw', 'wi_ar_trx_cr_asgn_non_app_trx') }}
),

final AS (
    SELECT
        source_deleted_flg,
        assignment_mode,
        copy_from_minx_order_number,
        copy_from_source_header_id,
        copy_from_source_type,
        created_by,
        creation_date,
        deal_scmt_id,
        ges_update_date,
        global_name,
        header_seq_id,
        interfaced_to_cdw_flag,
        last_updated_by,
        last_update_date,
        last_update_login,
        minx_order_number,
        salesrep_id,
        sales_credit_type_id,
        source_header_id,
        source_type,
        split_percent,
        territory_id
    FROM source_wi_ar_trx_cr_asgn_non_app_trx
)

SELECT * FROM final