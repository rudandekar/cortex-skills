{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_rw_cq_qute_ptnr_ex_kfk', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RW_CQ_QUTE_PTNR_EX_KFK',
        'target_table': 'ST_INT_RAW_CQ_QUOTE_PARTNER_EX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.902823+00:00'
    }
) }}

WITH 

source_el_int_rw_cq_qute_ptnr_ex_kfk AS (
    SELECT
        object_id,
        quote_object_id,
        partner_be_geo_id,
        partner_site_id,
        country_cd,
        partner_contact_pae_user_id,
        dm_update_date,
        created_by,
        created_on,
        updated_by,
        updated_on,
        edw_updated_date,
        crp_partner_conv_flag,
        deal_submitted_date,
        crp_partner_site_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        message_sequence_number
    FROM {{ source('raw', 'el_int_rw_cq_qute_ptnr_ex_kfk') }}
),

final AS (
    SELECT
        object_id,
        quote_object_id,
        partner_be_geo_id,
        partner_site_id,
        country_cd,
        partner_contact_pae_user_id,
        dm_update_date,
        created_by,
        created_on,
        updated_by,
        updated_on,
        edw_updated_date,
        crp_partner_conv_flag,
        deal_submitted_date,
        crp_partner_site_id
    FROM source_el_int_rw_cq_qute_ptnr_ex_kfk
)

SELECT * FROM final