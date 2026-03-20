{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_si_business_unit_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_SI_BUSINESS_UNIT_SRC2EL',
        'target_table': 'EL_SI_BUSINESS_UNIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.650852+00:00'
    }
) }}

WITH 

source_si_business_unit AS (
    SELECT
        business_unit_id,
        organization_lob_id,
        business_unit_value,
        created_by,
        create_date,
        last_updated_by,
        last_update_date,
        enabled_flag,
        business_unit_desc
    FROM {{ source('raw', 'si_business_unit') }}
),

transformed_exp_si_business_unit AS (
    SELECT
    business_unit_id,
    organization_lob_id,
    business_unit_value,
    last_update_date,
    enabled_flag,
    business_unit_desc,
    TO_INTEGER(BUSINESS_UNIT_ID) AS out_business_unit_id,
    TO_INTEGER(ORGANIZATION_LOB_ID) AS out_organization_lob_id
    FROM source_si_business_unit
),

lookup_lkp_el_si_business_unit AS (
    SELECT
        a.*,
        b.*
    FROM transformed_exp_si_business_unit a
    LEFT JOIN {{ source('raw', 'el_si_business_unit') }} b
        ON a.in_business_unit_id = b.in_business_unit_id
),

transformed_exp_el_si_business_unit AS (
    SELECT
    business_unit_id,
    organization_lob_id,
    business_unit_value,
    last_update_date,
    enabled_flag,
    business_unit_desc
    FROM lookup_lkp_el_si_business_unit
),

routed_rtr_el_si_business_unit AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exp_el_si_business_unit
),

update_strategy_ins_upd_si_business_unit AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtr_el_si_business_unit
    WHERE DD_INSERT != 3
),

update_strategy_upd_upd_si_business_unit AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_ins_upd_si_business_unit
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        business_unit_id,
        organization_lob_id,
        business_unit_value,
        last_update_date,
        enabled_flag,
        business_unit_desc
    FROM update_strategy_upd_upd_si_business_unit
)

SELECT * FROM final