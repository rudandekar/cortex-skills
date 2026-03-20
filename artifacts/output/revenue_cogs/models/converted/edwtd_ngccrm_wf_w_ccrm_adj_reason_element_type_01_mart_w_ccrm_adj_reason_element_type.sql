{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_adj_reason_element_type', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_ADJ_REASON_ELEMENT_TYPE',
        'target_table': 'W_CCRM_ADJ_REASON_ELEMENT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.657411+00:00'
    }
) }}

WITH 

source_st_ngccrm_reason_codes AS (
    SELECT
        batch_id,
        rc_id,
        rc_name,
        version,
        description,
        rc_type,
        rc_path,
        rc_flow,
        rc_acc_rule,
        rc_base,
        comments,
        start_date,
        end_date,
        p_rev_deferal,
        p_cogs_deferal,
        p_booking_deferal,
        p_ac_eligible,
        p_ab_eligible,
        p_cc_neligible,
        p_cct_neligible,
        ps_rev_deferal,
        ps_cogs_deferal,
        ps_booking_deferal,
        ps_ac_eligible,
        ps_ab_eligible,
        ps_cc_neligible,
        ps_cct_neligible,
        ass_rev_deferal,
        ass_cogs_deferal,
        ass_booking_deferal,
        ass_ac_eligible,
        ass_ab_eligible,
        ass_cc_neligible,
        ass_cct_neligible,
        tss_rev_deferal,
        tss_cogs_deferal,
        tss_booking_deferal,
        tss_ac_eligible,
        tss_ab_eligible,
        tss_cc_neligible,
        tss_cct_neligible,
        ast_rev_deferal,
        ast_cogs_deferal,
        ast_booking_deferal,
        ast_ac_eligible,
        ast_ab_eligible,
        ast_cc_neligible,
        ast_cct_neligible,
        os_rev_deferal,
        os_cogs_deferal,
        os_booking_deferal,
        os_ac_eligible,
        os_ab_eligible,
        os_cc_neligible,
        os_cct_neligible,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        status,
        latest_flag,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ngccrm_reason_codes') }}
),

final AS (
    SELECT
        bk_ccrm_adjustment_reason_cd,
        bk_ccrm_adj_reason_ver_num_int,
        bk_ccrm_element_type_cd,
        no_compnstn_until_deferred_flg,
        compnstn_calc_not_elig_flg,
        advance_booking_elig_flg,
        advance_compnstn_elig_flg,
        booking_deferral_flg,
        cogs_deferral_flg,
        revenue_deferral_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ngccrm_reason_codes
)

SELECT * FROM final