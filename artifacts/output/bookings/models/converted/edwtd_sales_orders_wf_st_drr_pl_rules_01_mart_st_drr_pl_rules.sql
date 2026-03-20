{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_drr_pl_rules', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_DRR_PL_RULES',
        'target_table': 'ST_DRR_PL_RULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.776185+00:00'
    }
) }}

WITH 

source_st_partner_rules AS (
    SELECT
        otm_level1,
        otm_level2,
        otm_level3,
        otm_level4,
        otm_level5,
        otm_level6,
        otm_level7,
        otm_level8,
        otm_level9,
        otm_level10,
        otm_level11,
        otm_level12,
        rank_level1,
        rank_level2,
        rank_level3,
        rank_level4,
        rank_level5,
        rank_level6,
        rank_level7,
        rank_level8,
        rank_level9,
        rank_level10,
        rank_level11,
        rank_level12,
        leaf_terr_id,
        leaf_terr_name,
        absolute_rank,
        qualifier_name,
        qualifier_condition,
        qualifier_value,
        split,
        sales_agent_number,
        sales_agent,
        node_id,
        osh_territory_type,
        assignment_start_date,
        assignment_end_date
    FROM {{ source('raw', 'st_partner_rules') }}
),

final AS (
    SELECT
        otm_level1,
        otm_level2,
        otm_level3,
        otm_level4,
        otm_level5,
        otm_level6,
        otm_level7,
        otm_level8,
        otm_level9,
        otm_level10,
        otm_level11,
        otm_level12,
        rank_level1,
        rank_level2,
        rank_level3,
        rank_level4,
        rank_level5,
        rank_level6,
        rank_level7,
        rank_level8,
        rank_level9,
        rank_level10,
        rank_level11,
        rank_level12,
        leaf_terr_id,
        leaf_terr_name,
        absolute_rank,
        qualifier_name,
        qualifier_condition,
        qualifier_value,
        split,
        sales_agent_number,
        sales_agent,
        node_id,
        osh_territory_type,
        assignment_start_date,
        assignment_end_date
    FROM source_st_partner_rules
)

SELECT * FROM final