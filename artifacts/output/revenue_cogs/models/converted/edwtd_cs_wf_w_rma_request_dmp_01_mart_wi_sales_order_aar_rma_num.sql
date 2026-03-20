{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rma_request_dmp', 'batch', 'edwtd_cs'],
    meta={
        'source_workflow': 'wf_m_W_RMA_REQUEST_DMP',
        'target_table': 'WI_SALES_ORDER_AAR_RMA_NUM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.977776+00:00'
    }
) }}

WITH 

source_wi_sales_order_aar_rma_num AS (
    SELECT
        sales_order_key,
        sales_order_line_rma_aar_num
    FROM {{ source('raw', 'wi_sales_order_aar_rma_num') }}
),

source_w_rma_request AS (
    SELECT
        bk_awaiting_authorization_num,
        rma_request_approval_reqd_role,
        rma_request_submit_dtm,
        dv_rma_request_submit_dt,
        rma_request_type_cd,
        rma_request_status_cd,
        rma_requestor_email_addr,
        rma_requestor_cco_id,
        original_sales_order_key,
        customer_return_cmt_txt,
        queue_admin_cmt_txt,
        bk_customer_return_reason_cd,
        approved_rma_sales_order_key,
        ru_apprvr_area_ctrlr_prty_key,
        ru_apprvr_rgnl_mgr_prty_key,
        ru_apprvr_director_prty_key,
        ru_rma_req_denial_reason_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        last_modified_dtm,
        dv_last_modified_dt,
        restocking_fee_pct,
        rma_approved_dtm,
        dv_rma_approved_dt,
        crm_case_num,
        rqst_created_by_name,
        expedite_flg,
        hold_flg,
        xx_approver_id,
        xx_approved_by_proxy_id,
        action_code,
        dml_type,
        denial_reason_code,
        ru_apprvr_fin_vp_prty_key
    FROM {{ source('raw', 'w_rma_request') }}
),

source_gg_dmp_co_return_contact AS (
    SELECT
        contact_id,
        return_id,
        reference_type,
        contact_first_name,
        contact_last_name,
        contact_email,
        contact_phone,
        contact_fax,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'gg_dmp_co_return_contact') }}
),

source_ex_dmp_co_return_header AS (
    SELECT
        batch_id,
        return_id,
        aar_number,
        return_status,
        active,
        return_reason,
        return_from_country_cd,
        return_type,
        orig_deal_id,
        orig_web_order_id,
        orig_sales_order_number,
        orig_erp_header_id,
        orig_purchase_order_number,
        orig_sales_order_booked_date,
        org_id,
        invoice_to_site_use_id,
        ship_to_site_use_id,
        price_list_id,
        currency_code,
        application_flow,
        restocking_fee_percentage,
        additional_return_notes,
        order_total,
        submitted_on,
        submitted_by,
        submitted_by_proxy,
        created_on,
        created_by_proxy,
        created_by,
        updated_on,
        updated_by,
        csops_case_number,
        csops_notes,
        csops_customer_notes,
        share_node_id,
        application_sub_flow,
        create_datetime,
        source_commit_time,
        refresh_datetime,
        action_code,
        exception_type,
        expedite_flag,
        hold_flag
    FROM {{ source('raw', 'ex_dmp_co_return_header') }}
),

sorted_srttrans AS (
    SELECT *
    FROM source_ex_dmp_co_return_header
    ORDER BY 1
),

aggregated_aggtrans AS (
    SELECT
    return_id,
    email,
    MAX(EMAIL) AS aggr_email
    FROM sorted_srttrans
    GROUP BY return_id, email
),

transformed_exptrans AS (
    SELECT
    return_id,
    contact_email,
    IFF(RETURN_ID=OLD_RETURN_ID,OUTPUT_EMAIL||','||CONTACT_EMAIL,CONTACT_EMAIL) AS output_email,
    RETURN_ID AS old_return_id,
    OUTPUT_EMAIL AS email
    FROM aggregated_aggtrans
),

final AS (
    SELECT
        sales_order_key,
        sales_order_line_rma_aar_num
    FROM transformed_exptrans
)

SELECT * FROM final