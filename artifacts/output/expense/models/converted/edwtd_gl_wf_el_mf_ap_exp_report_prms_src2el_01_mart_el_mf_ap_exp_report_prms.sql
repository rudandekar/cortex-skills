{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_mf_ap_exp_report_prms_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_MF_AP_EXP_REPORT_PRMS_SRC2EL',
        'target_table': 'EL_MF_AP_EXP_REPORT_PRMS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.649078+00:00'
    }
) }}

WITH 

source_mf_ap_exp_report_prms AS (
    SELECT
        amount_includes_tax_flag,
        calculate_amount_flag,
        card_exp_type_lookup_code,
        category_code,
        company_policy_id,
        created_by,
        creation_date,
        data_capture_rule_id,
        end_date,
        expense_report_id,
        expense_type_code,
        flex_ccid,
        flex_concactenated,
        flex_description,
        ges_update_date,
        global_name,
        itemization_all_flag,
        itemization_required_flag,
        justification_required_flag,
        last_updated_by,
        last_update_date,
        last_update_login,
        line_type_lookup_code,
        org_id,
        parameter_id,
        pa_expenditure_type,
        prompt,
        receipt_required_flag,
        require_receipt_amount,
        summary_flag,
        vat_code,
        web_enabled_flag,
        web_friendly_prompt,
        web_image_filename,
        web_sequence
    FROM {{ source('raw', 'mf_ap_exp_report_prms') }}
),

transformed_exptrans1 AS (
    SELECT
    pa_expenditure_type,
    global_name,
    parameter_id,
    web_friendly_prompt,
    prompt,
    created_by,
    creation_date,
    ges_update_date,
    last_updated_by,
    last_update_date,
    company_policy_id,
    TO_INTEGER(PARAMETER_ID) AS out_parameter_id,
    TO_INTEGER(LAST_UPDATED_BY) AS out_last_updated_by
    FROM source_mf_ap_exp_report_prms
),

update_strategy_upd_updtrans AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exptrans1
    WHERE DD_UPDATE != 3
),

lookup_lkptrans AS (
    SELECT
        a.*,
        b.*
    FROM update_strategy_upd_updtrans a
    LEFT JOIN {{ source('raw', 'el_mf_ap_exp_report_prms') }} b
        ON a.in_global_name = b.in_global_name
),

routed_rtrtrans AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM lookup_lkptrans
),

transformed_exptrans AS (
    SELECT
    pa_expenditure_type,
    global_name,
    parameter_id,
    web_friendly_prompt,
    prompt,
    created_by,
    creation_date,
    ges_update_date,
    last_updated_by,
    last_update_date,
    company_policy_id,
    lkp_pa_expenditure_type,
    lkp_global_name,
    lkp_parameter_id,
    lkp_web_friendly_prompt,
    lkp_prompt,
    lkp_created_by,
    lkp_creation_date,
    lkp_ges_update_date,
    lkp_last_updated_by,
    lkp_last_update_date,
    lkp_company_policy_id
    FROM routed_rtrtrans
),

update_strategy_ins_updtrans AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exptrans
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        pa_expenditure_type,
        global_name,
        parameter_id,
        web_friendly_prompt,
        prompt,
        created_by,
        creation_date,
        ges_update_date,
        last_updated_by,
        last_update_date,
        company_policy_id
    FROM update_strategy_ins_updtrans
)

SELECT * FROM final