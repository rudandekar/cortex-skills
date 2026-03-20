{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_cm_pull_in_message', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CM_PULL_IN_MESSAGE',
        'target_table': 'SM_CM_PULL_IN_MESSAGE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.661756+00:00'
    }
) }}

WITH 

source_st_cg1_xscm_pl_pbm_recmit_data AS (
    SELECT
        message_id,
        organization_code,
        order_number,
        ship_set_number,
        line_number,
        pid,
        order_date_type_code,
        improved_tans,
        current_ssd,
        recommended_ssd,
        current_sad,
        new_sad,
        current_pdate,
        suggested_pdate,
        recommit_reason,
        cisco_detail_recommit_reason,
        pull_in_quantity,
        pull_in_comments,
        amp_user_name,
        process_status,
        error_code,
        error_message,
        transmission_date,
        old_pdate_fiscal_week,
        new_pdate_fiscal_week,
        trans_date_fiscal_week,
        ship_set_status,
        secondary_priority,
        ss_days_improved,
        recommit_date,
        last_update_date,
        created_by,
        creation_date,
        request_id,
        last_update_login,
        last_updated_by,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute16,
        attribute17,
        attribute18,
        attribute19,
        attribute20,
        global_name,
        create_datetime
    FROM {{ source('raw', 'st_cg1_xscm_pl_pbm_recmit_data') }}
),

final AS (
    SELECT
        cm_pull_in_message_key,
        sk_message_id,
        sales_order_line_num_int,
        sales_order_num_int,
        edw_create_dtm,
        edw_create_user
    FROM source_st_cg1_xscm_pl_pbm_recmit_data
)

SELECT * FROM final