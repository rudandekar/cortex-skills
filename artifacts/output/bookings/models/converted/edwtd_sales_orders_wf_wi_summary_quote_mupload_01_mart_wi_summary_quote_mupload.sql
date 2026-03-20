{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_summary_quote_mupload', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SUMMARY_QUOTE_MUPLOAD',
        'target_table': 'WI_SUMMARY_QUOTE_MUPLOAD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.558089+00:00'
    }
) }}

WITH 

source_wi_summary_quote_mupload AS (
    SELECT
        item_category_descr,
        bk_so_line_id_int,
        sales_order_line_key,
        bk_so_number_int,
        bk_service_quote_num,
        dv_maint_net_local_total_amt,
        dv_allocation_pct,
        sales_motion_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_effective_dtm,
        attributed_product,
        sk_offer_attribution_id_int,
        as_ts_flg,
        as_architecture_name,
        technology_group_id,
        attr_prdt_offer_type_name,
        item_categorization,
        reason_code,
        case_number,
        comments,
        upload_id,
        manual_override_flag
    FROM {{ source('raw', 'wi_summary_quote_mupload') }}
),

final AS (
    SELECT
        item_category_descr,
        bk_so_line_id_int,
        sales_order_line_key,
        bk_so_number_int,
        bk_service_quote_num,
        dv_maint_net_local_total_amt,
        dv_allocation_pct,
        sales_motion_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_effective_dtm,
        attributed_product,
        sk_offer_attribution_id_int,
        as_ts_flg,
        as_architecture_name,
        technology_group_id,
        attr_prdt_offer_type_name,
        item_categorization,
        reason_code,
        case_number,
        comments,
        upload_id,
        manual_override_flag
    FROM source_wi_summary_quote_mupload
)

SELECT * FROM final