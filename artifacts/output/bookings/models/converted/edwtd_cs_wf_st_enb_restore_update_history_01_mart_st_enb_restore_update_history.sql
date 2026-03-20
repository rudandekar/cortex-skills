{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_enb_restore_update_history', 'batch', 'edwtd_cs'],
    meta={
        'source_workflow': 'wf_m_ST_ENB_RESTORE_UPDATE_HISTORY',
        'target_table': 'ST_ENB_RESTORE_UPDATE_HISTORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.696642+00:00'
    }
) }}

WITH 

source_ff_enb_restore_update_history AS (
    SELECT
        batch_id,
        action_code,
        create_datetime,
        sales_order_key,
        sales_order_num,
        ro_reason_descr,
        ro_cmnt_txt,
        gsd_reason_descr,
        gsd_cmnt_txt,
        owner_agent_name,
        issue_flg,
        booked_current_week_flg,
        sfdc_case_detail_txt,
        reassigned_sales_terr_name,
        reassigned_sales_agent_cec_id,
        reassigned_sls_agent_user_name,
        last_updated_cec_id,
        entered_on_dtm
    FROM {{ source('raw', 'ff_enb_restore_update_history') }}
),

final AS (
    SELECT
        batch_id,
        action_code,
        create_datetime,
        sales_order_key,
        sales_order_num,
        ro_reason_descr,
        ro_cmnt_txt,
        gsd_reason_descr,
        gsd_cmnt_txt,
        owner_agent_name,
        issue_flg,
        booked_current_week_flg,
        sfdc_case_detail_txt,
        reassigned_sales_terr_name,
        reassigned_sales_agent_cec_id,
        reassigned_sls_agent_user_name,
        last_updated_cec_id,
        entered_on_dtm
    FROM source_ff_enb_restore_update_history
)

SELECT * FROM final