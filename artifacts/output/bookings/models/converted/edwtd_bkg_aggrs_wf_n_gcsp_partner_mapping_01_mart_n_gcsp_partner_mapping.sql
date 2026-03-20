{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_gcsp_partner_mapping', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_N_GCSP_PARTNER_MAPPING',
        'target_table': 'N_GCSP_PARTNER_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.817018+00:00'
    }
) }}

WITH 

source_n_gcsp_partner_mapping AS (
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
        bk_operating_unit_name_cd
    FROM {{ source('raw', 'n_gcsp_partner_mapping') }}
),

final AS (
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
        bk_operating_unit_name_cd
    FROM source_n_gcsp_partner_mapping
)

SELECT * FROM final