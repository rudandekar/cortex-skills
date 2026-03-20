{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wips_pos_svc_details_pub', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_POS_SVC_DETAILS_PUB',
        'target_table': 'ST_WIPS_POS_SVC_DETAILS_PUB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.413844+00:00'
    }
) }}

WITH 

source_ff_wips_pos_svc_details_pub AS (
    SELECT
        trans_id,
        id,
        booking_flag,
        service_type,
        active_flag,
        created_date,
        created_by,
        last_updated_date,
        last_updated_by,
        action_code,
        derived_quote_number,
        derived_contract_number,
        derived_quote_line_id,
        derived_order_line_id,
        match_type,
        order_header_id,
        quote_header_id,
        revenue_source_code,
        confidence_flag,
        po_exists_flg,
        po_distri_exists_flg,
        po_prdt_exists_flg,
        quote_exists_flg,
        quote_distri_exists_flg,
        quote_prdt_exists_flg,
        multiple_sales_motion_flg,
        multiple_match_flg,
        approved_erp_linkage_flg,
        order_identifier
    FROM {{ source('raw', 'ff_wips_pos_svc_details_pub') }}
),

final AS (
    SELECT
        trans_id,
        id,
        booking_flag,
        service_type,
        active_flag,
        created_date,
        created_by,
        last_updated_date,
        last_updated_by,
        action_code,
        derived_quote_number,
        derived_contract_number,
        derived_quote_line_id,
        derived_order_line_id,
        match_type,
        order_header_id,
        quote_header_id,
        revenue_source_code,
        confidence_flag,
        po_exists_flg,
        po_distri_exists_flg,
        po_prdt_exists_flg,
        quote_exists_flg,
        quote_distri_exists_flg,
        quote_prdt_exists_flg,
        multiple_sales_motion_flg,
        multiple_match_flg,
        approved_erp_linkage_flg,
        order_identifier
    FROM source_ff_wips_pos_svc_details_pub
)

SELECT * FROM final