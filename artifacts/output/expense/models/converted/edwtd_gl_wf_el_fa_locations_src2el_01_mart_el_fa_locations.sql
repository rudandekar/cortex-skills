{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_fa_locations_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_FA_LOCATIONS_SRC2EL',
        'target_table': 'EL_FA_LOCATIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.110989+00:00'
    }
) }}

WITH 

source_mf_fa_locations AS (
    SELECT
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category_code,
        enabled_flag,
        end_date_active,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        location_id,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        segment7,
        start_date_active,
        summary_flag
    FROM {{ source('raw', 'mf_fa_locations') }}
),

lookup_lkp_el_fa_locations AS (
    SELECT
        a.*,
        b.*
    FROM source_mf_fa_locations a
    LEFT JOIN {{ source('raw', 'el_fa_locations') }} b
        ON a.in_global_name = b.in_global_name
),

transformed_exp_mf_fa_locations AS (
    SELECT
    global_name,
    location_id,
    segment1,
    segment2,
    segment3,
    TO_INTEGER(LOCATION_ID) AS out_location_id
    FROM lookup_lkp_el_fa_locations
),

transformed_exp_el_fa_locations AS (
    SELECT
    global_name,
    location_id,
    segment1,
    segment2,
    segment3,
    lkp_global_name,
    lkp_location_id,
    lkp_segment1,
    lkp_segment2,
    lkp_segment3
    FROM transformed_exp_mf_fa_locations
),

routed_rtr_el_fa_locations AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exp_el_fa_locations
),

update_strategy_ins_upd_el_fa_locations AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtr_el_fa_locations
    WHERE DD_INSERT != 3
),

update_strategy_upd_upd_el_fa_locations AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_ins_upd_el_fa_locations
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        global_name,
        location_id,
        segment1,
        segment2,
        segment3
    FROM update_strategy_upd_upd_el_fa_locations
)

SELECT * FROM final