{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxnce_claim', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_XXNCE_CLAIM',
        'target_table': 'ST_XXNCE_CLAIM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.345395+00:00'
    }
) }}

WITH 

source_xxnce_claim AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        claim_id,
        claim_status,
        territory_type_code,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        object_version_number,
        claim_reason_code,
        claim_comment,
        claim_value
    FROM {{ source('raw', 'xxnce_claim') }}
),

final AS (
    SELECT
        claim_id,
        claim_status,
        territory_type_code,
        creation_date,
        created_by,
        last_updated_date,
        last_updated_by,
        object_version_number,
        claim_reason_code,
        claim_comment,
        claim_value
    FROM source_xxnce_claim
)

SELECT * FROM final