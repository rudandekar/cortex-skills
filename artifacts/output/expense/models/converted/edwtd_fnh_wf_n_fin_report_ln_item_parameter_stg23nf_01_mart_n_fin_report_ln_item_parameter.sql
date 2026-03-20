{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_report_ln_item_parameter_stg23nf', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_N_FIN_REPORT_LN_ITEM_PARAMETER_STG23NF',
        'target_table': 'N_FIN_REPORT_LN_ITEM_PARAMETER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.007213+00:00'
    }
) }}

WITH 

source_st_n_financial_rli_mapping AS (
    SELECT
        batch_id,
        rli_member,
        rli_member_desc,
        entity_low,
        entity_high,
        location_low,
        location_high,
        department_low,
        department_high,
        account_low,
        account_high,
        refresh_date,
        create_datetime,
        action_code,
        inclusion_flag
    FROM {{ source('raw', 'st_n_financial_rli_mapping') }}
),

transformed_exptrans1 AS (
    SELECT
    bk_location_range_low_cd,
    bk_account_range_low_cd,
    bk_company_range_low_cd,
    bk_department_range_low_cd,
    bk_finance_report_line_item_cd,
    bk_location_range_high_cd,
    bk_account_range_high_cd,
    bk_company_range_high_cd,
    bk_department_range_high_cd,
    inclusion_flag
    FROM source_st_n_financial_rli_mapping
),

update_strategy_upd_ins AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exptrans1
    WHERE DD_INSERT != 3
),

update_strategy_upd_upd AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_ins
    WHERE DD_UPDATE != 3
),

transformed_exptrans AS (
    SELECT
    bk_location_range_low_cd,
    bk_account_range_low_cd,
    bk_company_range_low_cd,
    bk_department_range_low_cd,
    bk_finance_report_line_item_cd,
    bk_location_range_high_cd,
    bk_account_range_high_cd,
    bk_company_range_high_cd,
    bk_department_range_high_cd,
    inclusion_flag,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM update_strategy_upd_upd
),

final AS (
    SELECT
        bk_location_range_low_cd,
        bk_account_range_low_cd,
        bk_company_range_low_cd,
        bk_department_range_low_cd,
        bk_finance_report_line_item_cd,
        bk_location_range_high_cd,
        bk_account_range_high_cd,
        bk_company_range_high_cd,
        bk_department_range_high_cd,
        inclusion_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM transformed_exptrans
)

SELECT * FROM final