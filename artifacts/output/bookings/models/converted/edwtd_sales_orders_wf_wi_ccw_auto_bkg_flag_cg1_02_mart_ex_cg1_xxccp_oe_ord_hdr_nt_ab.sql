{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccw_auto_bkg_flag_cg1', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_CCW_AUTO_BKG_FLAG_CG1',
        'target_table': 'EX_CG1_XXCCP_OE_ORD_HDR_NT_AB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.086626+00:00'
    }
) }}

WITH 

source_st_cg1_xxccp_oe_ord_hdr_nt_ab AS (
    SELECT
        xxccp_header_id,
        org_id,
        status,
        sub_status,
        erp_header_id,
        order_number,
        operation,
        global_name,
        quote_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_commit_time
    FROM {{ source('raw', 'st_cg1_xxccp_oe_ord_hdr_nt_ab') }}
),

source_ex_cg1_xxccp_oe_ord_hdr_nt_ab AS (
    SELECT
        xxccp_header_id,
        org_id,
        status,
        sub_status,
        erp_header_id,
        order_number,
        operation,
        global_name,
        quote_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_commit_time,
        exception_type
    FROM {{ source('raw', 'ex_cg1_xxccp_oe_ord_hdr_nt_ab') }}
),

final AS (
    SELECT
        xxccp_header_id,
        org_id,
        status,
        sub_status,
        erp_header_id,
        order_number,
        operation,
        global_name,
        quote_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_commit_time,
        exception_type
    FROM source_ex_cg1_xxccp_oe_ord_hdr_nt_ab
)

SELECT * FROM final