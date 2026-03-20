{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_drr_pl_rules_denorm', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRR_PL_RULES_DENORM',
        'target_table': 'WI_DRR_PL_RULES_DENORM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.279781+00:00'
    }
) }}

WITH 

source_wi_drr_pl_rules_denorm AS (
    SELECT
        leaf_terr_id,
        leaf_terr_name,
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
        absolute_rank,
        split,
        pr_sales_rep_number,
        pr_sales_territory_key,
        postal_cd_max_range,
        share_short_ctry_cd,
        share_country_cd,
        pgtmv_geo_id,
        end_ship_to_contry_id,
        end_ship_to_postal_cd_range,
        end_ship_to_postal_cd_lov,
        end_ship_to_postal_cd_init,
        dv_end_ship_to_postal_cd_init,
        end_ship_to_state,
        distributor_id,
        end_ship_to_city_id,
        end_sold_to_country_id,
        pgtmv_partner_be_id,
        process_type,
        year_flg_int
    FROM {{ source('raw', 'wi_drr_pl_rules_denorm') }}
),

final AS (
    SELECT
        leaf_terr_id,
        leaf_terr_name,
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
        absolute_rank,
        split,
        pr_sales_rep_number,
        pr_sales_territory_key,
        postal_cd_max_range,
        share_short_ctry_cd,
        share_country_cd,
        pgtmv_geo_id,
        end_ship_to_contry_id,
        end_ship_to_postal_cd_range,
        end_ship_to_postal_cd_lov,
        end_ship_to_postal_cd_init,
        dv_end_ship_to_postal_cd_init,
        end_ship_to_state,
        distributor_id,
        end_ship_to_city_id,
        end_sold_to_country_id,
        pgtmv_partner_be_id,
        process_type,
        year_flg_int
    FROM source_wi_drr_pl_rules_denorm
)

SELECT * FROM final