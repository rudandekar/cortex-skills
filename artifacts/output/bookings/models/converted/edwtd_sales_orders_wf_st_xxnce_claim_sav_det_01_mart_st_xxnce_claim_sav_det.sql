{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxnce_claim_sav_det', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_XXNCE_CLAIM_SAV_DET',
        'target_table': 'ST_XXNCE_CLAIM_SAV_DET',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.149910+00:00'
    }
) }}

WITH 

source_xxnce_claim_sav_det AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        sav_det_id,
        sav_det_seq,
        sav_id,
        sav_name,
        biz_party_id,
        biz_party_name,
        sav_split_pct,
        end_user_party_id,
        end_user_party_name,
        created_by,
        creation_date,
        last_updated_by,
        updated_date,
        object_version_number,
        claim_id,
        end_user_country
    FROM {{ source('raw', 'xxnce_claim_sav_det') }}
),

final AS (
    SELECT
        sav_det_id,
        sav_det_seq,
        sav_id,
        sav_name,
        biz_party_id,
        biz_party_name,
        sav_split_pct,
        end_user_party_id,
        end_user_party_name,
        created_by,
        creation_date,
        last_updated_by,
        updated_date,
        object_version_number,
        claim_id,
        end_user_country
    FROM source_xxnce_claim_sav_det
)

SELECT * FROM final