{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_gcsp_mapping', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_ST_GCSP_MAPPING',
        'target_table': 'GCSP_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.143566+00:00'
    }
) }}

WITH 

source_dpp_gcsp_pdb_ou_ccw_edw AS (
    SELECT
        edw_seq_no,
        be_geo_id,
        be_geo_name,
        partner_country,
        end_customer_gu_id,
        end_customer_gu_name,
        end_customer_country,
        partner_bill_to_id,
        partner_bill_to_country,
        ou_id,
        ou_territory_cd,
        last_updated_on,
        active_flag,
        start_date,
        end_date,
        be_id
    FROM {{ source('raw', 'dpp_gcsp_pdb_ou_ccw_edw') }}
),

final AS (
    SELECT
        edw_seq_no,
        be_geo_id,
        be_geo_name,
        partner_country,
        end_customer_gu_id,
        end_customer_gu_name,
        end_customer_country,
        partner_bill_to_id,
        partner_bill_to_country,
        ou_id,
        ou_territory_cd,
        last_updated_on,
        active_flag,
        start_date,
        end_date,
        be_id
    FROM source_dpp_gcsp_pdb_ou_ccw_edw
)

SELECT * FROM final