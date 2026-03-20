{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sc_inv_org_rprtng_structure_sop23nf', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SC_INV_ORG_RPRTNG_STRUCTURE_SOP23NF',
        'target_table': 'N_SC_INV_ORG_RPRTNG_STRUCTURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.492064+00:00'
    }
) }}

WITH 

source_sc_inv_org_rprtng_structure AS (
    SELECT
        org,
        sub_region,
        region,
        dro,
        vp,
        partner,
        partner_type,
        row_id,
        line_id,
        batch_id
    FROM {{ source('raw', 'sc_inv_org_rprtng_structure') }}
),

transformed_exp_sc_inv_org_rprtng_structure AS (
    SELECT
    inventory_organization_key,
    director_name,
    vice_president_name,
    partner_name,
    partner_type_cd,
    region_cd,
    subregion_cd
    FROM source_sc_inv_org_rprtng_structure
),

update_strategy_ins_sc_inv_org_rprtng_structure AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_sc_inv_org_rprtng_structure
    WHERE DD_INSERT != 3
),

transformed_exp_sc_inv_org_rprtng_structure1 AS (
    SELECT
    inventory_organization_key,
    director_name,
    vice_president_name,
    partner_name,
    partner_type_cd,
    region_cd,
    subregion_cd
    FROM update_strategy_ins_sc_inv_org_rprtng_structure
),

update_strategy_upd_sc_inv_org_rprtng_structure1 AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_sc_inv_org_rprtng_structure1
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        inventory_organization_key,
        director_name,
        vice_president_name,
        partner_name,
        partner_type_cd,
        region_cd,
        subregion_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_sc_inv_org_rprtng_structure1
)

SELECT * FROM final