{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_rw_cq_qute_ptr_ex_kfk_f', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_RW_CQ_QUTE_PTR_EX_KFK_F',
        'target_table': 'ST_INT_RW_CQ_QUTE_PTR_EX_KFK_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.962599+00:00'
    }
) }}

WITH 

source_ff_st_int_raw_cq_quote_partner_ex_kafka AS (
    SELECT
        parent_id,
        created_on,
        updated_on,
        object_id,
        partner_be_geo_id,
        partner_site_id,
        dm_update_date,
        country_cd,
        partner_contact_pae_user_id,
        deal_submitted_date,
        pdb_site_id,
        created_by,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'ff_st_int_raw_cq_quote_partner_ex_kafka') }}
),

final AS (
    SELECT
        parent_id,
        created_on,
        updated_on,
        object_id,
        partner_be_geo_id,
        partner_site_id,
        dm_update_date,
        country_cd,
        partner_contact_pae_user_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number,
        deal_submitted_date,
        pdb_site_id,
        created_by,
        edw_updated_date
    FROM source_ff_st_int_raw_cq_quote_partner_ex_kafka
)

SELECT * FROM final