{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_rw_cq_qute_ptnr_ex_kfk', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_RW_CQ_QUTE_PTNR_EX_KFK',
        'target_table': 'EL_INT_RW_CQ_QUTE_PTNR_EX_KFK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.985493+00:00'
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
        pdb_site_id,
        crp_partner_site_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
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
        pdb_site_id,
        crp_partner_site_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        edw_create_dtm,
        edw_create_user,
        partition_number,
        message_sequence_number,
        edw_update_dtm,
        edw_update_user
    FROM source_el_int_rw_cq_qute_ptnr_ex_kfk
)

SELECT * FROM final