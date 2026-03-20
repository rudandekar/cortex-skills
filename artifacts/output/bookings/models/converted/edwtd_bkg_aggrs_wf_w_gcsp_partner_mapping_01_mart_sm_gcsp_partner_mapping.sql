{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_gcsp_partner_mapping', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_W_GCSP_PARTNER_MAPPING',
        'target_table': 'SM_GCSP_PARTNER_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.588121+00:00'
    }
) }}

WITH 

source_sm_gcsp_partner_mapping AS (
    SELECT
        gcsp_partner_mapping_key,
        sk_seq_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_gcsp_partner_mapping') }}
),

source_w_gcsp_partner_mapping AS (
    SELECT
        gcsp_partner_mapping_key,
        be_id,
        sk_seq_id,
        be_geo_id,
        be_geo_name,
        partner_iso_cntry_name,
        end_cust_gu_id,
        end_cust_iso_cntry_name,
        end_cust_gu_name,
        partner_bill_to_id,
        partner_bill_to_cntry_name,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        start_dtm,
        dv_start_dt,
        end_dtm,
        dv_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_gcsp_partner_mapping') }}
),

final AS (
    SELECT
        gcsp_partner_mapping_key,
        sk_seq_id,
        edw_create_dtm,
        edw_create_user
    FROM source_w_gcsp_partner_mapping
)

SELECT * FROM final