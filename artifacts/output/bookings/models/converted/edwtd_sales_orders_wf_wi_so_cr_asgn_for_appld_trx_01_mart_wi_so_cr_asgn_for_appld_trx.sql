{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_so_cr_asgn_for_appld_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SO_CR_ASGN_FOR_APPLD_TRX',
        'target_table': 'WI_SO_CR_ASGN_FOR_APPLD_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.356088+00:00'
    }
) }}

WITH 

source_st_om_csm_hdr_sc_appld AS (
    SELECT
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
    FROM {{ source('raw', 'st_om_csm_hdr_sc_appld') }}
),

source_st_om_csm_hdr_sc_appld_del AS (
    SELECT
        assignment_mode,
        copy_from_minx_order_number,
        copy_from_source_header_id,
        copy_from_source_type,
        created_by,
        creation_date,
        deal_scmt_id,
        ges_delete_date,
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
    FROM {{ source('raw', 'st_om_csm_hdr_sc_appld_del') }}
),

final AS (
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
    FROM source_st_om_csm_hdr_sc_appld_del
)

SELECT * FROM final