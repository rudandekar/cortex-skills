{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_quote_concession_source', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_QUOTE_CONCESSION_SOURCE',
        'target_table': 'W_DEAL_QUOTE_CONCESSION_SOURCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.953439+00:00'
    }
) }}

WITH 

source_st_int_raw_cq_quote_lne_conces AS (
    SELECT
        object_id,
        discount_id,
        post_term_clause_flag,
        applied_product_family_id,
        promo_version,
        appr_ns_discount_pctg,
        req_ns_discount_pctg,
        modifier_line_id,
        disc_percentage,
        disc_amount,
        price_used_for_adjustment,
        net_price_aft_adjustment,
        pricing_phase_code,
        modifieder_line_type_code,
        list_line_number,
        include_on_return_flag,
        disc_method,
        modifier_level_code,
        bucket,
        automatic_flag,
        update_allowable_flag,
        updated_flag,
        applied_flag,
        print_on_invoice_flag,
        change_reason_code,
        change_reason_text,
        promotion_code,
        promotion_name,
        channer_prog_allignment,
        modified_from,
        modified_to,
        estimated_flag,
        charge_type_code,
        charge_subtype_code,
        range_break_quantity,
        accural_conversion_rate,
        accural_flag,
        benefit_quantity,
        benefit_uom_code,
        price_break_type_code,
        substitution_attribute,
        proration_type_code,
        credit_or_charge_flag,
        inc_in_sales_performance,
        rebate_transaction_type_code,
        rebate_transaction_reference,
        rebate_payment_system_code,
        operand_per_quantity,
        adjusted_amt_per_quantity,
        expiration_date,
        list_line_id,
        disc_name,
        adjust_price_list_amt,
        adjusted_amount,
        updated_date,
        updated_by,
        creation_dt,
        created_by,
        modifier_header_id,
        quote_line_object_id,
        concession_id,
        concession_type,
        concession_value,
        concession_value_type,
        concession_applied,
        discount_version,
        edw_updated_date
    FROM {{ source('raw', 'st_int_raw_cq_quote_lne_conces') }}
),

final AS (
    SELECT
        bk_quote_num,
        bk_deal_concession_source_num,
        concession_source_type,
        ru_tmip_quote_num,
        bk_contract_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_int_raw_cq_quote_lne_conces
)

SELECT * FROM final