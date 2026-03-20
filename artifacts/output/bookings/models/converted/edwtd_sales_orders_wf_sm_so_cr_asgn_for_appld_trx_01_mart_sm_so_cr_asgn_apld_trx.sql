{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_so_cr_asgn_for_appld_trx', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_SO_CR_ASGN_FOR_APPLD_TRX',
        'target_table': 'SM_SO_CR_ASGN_APLD_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.866164+00:00'
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

transformed_exptrans AS (
    SELECT
    so_credit_asgn_applied_key,
    header_seq_id,
    global_name,
    edw_create_dtm,
    edw_create_user
    FROM source_st_om_csm_hdr_sc_appld_del
),

final AS (
    SELECT
        so_credit_asgn_applied_key,
        sk_header_sequence_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM transformed_exptrans
)

SELECT * FROM final