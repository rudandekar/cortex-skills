{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_slc_site_cntnr_inv_outbound', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_SLC_SITE_CNTNR_INV_OUTBOUND',
        'target_table': 'W_SLC_SITE_CNTNR_INV_OUTBOUND',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.860676+00:00'
    }
) }}

WITH 

source_st_cg1_xxscm_dv_4c1_ob_iface AS (
    SELECT
        message_id,
        cycle_count_id,
        organization_name,
        order_number,
        ship_set_number,
        slc_site_name,
        requested_date,
        age_bucket,
        percent_evaluation,
        selected_order_shipset,
        message_status,
        handling_unit_id,
        random_number,
        part_duns_number,
        cisco_duns_number,
        retry_count,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
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
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_xxscm_dv_4c1_ob_iface') }}
),

final AS (
    SELECT
        bk_strtgc_logistics_cntr_name,
        bk_shipset_num_int,
        sales_order_key,
        bk_carton_id,
        bk_requested_dtm,
        with_inbound_role,
        creation_dtm,
        ru_bk_inbound_response_dtm,
        dv_creation_dt,
        age_bucket_cnt,
        dv_requested_dt,
        sk_cycle_id_int_cnt,
        sk_message_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_cg1_xxscm_dv_4c1_ob_iface
)

SELECT * FROM final