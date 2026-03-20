{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_direct_indirect_adj_type ', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_MT_DIRECT_INDIRECT_ADJ_TYPE ',
        'target_table': 'MT_DIRECT_INDIRECT_ADJ_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.826173+00:00'
    }
) }}

WITH 

source_n_direct_corp_adj_type_lvl1 AS (
    SELECT
        bk_direct_corp_adj_lvl1_typ_cd,
        direct_corp_adj_lvl1_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_direct_corp_adj_type_lvl1') }}
),

source_n_direct_corp_adj_type AS (
    SELECT
        bk_direct_corp_adj_type_cd,
        bk_direct_corp_adj_lvl2_typ_cd,
        direct_corp_adj_end_dtm,
        direct_corp_adj_start_dtm,
        direct_corp_adj_descr,
        sk_adj_type_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_direct_corp_adj_type') }}
),

source_n_direct_corp_adj_type_lvl2 AS (
    SELECT
        bk_direct_corp_adj_lvl2_typ_cd,
        bk_direct_corp_adj_lvl1_typ_cd,
        direct_corp_adj_lvl2_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_direct_corp_adj_type_lvl2') }}
),

source_n_indrct_rev_cogs_adj_type AS (
    SELECT
        bk_adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        adj_type_effective_start_dtm,
        adj_type_effective_end_dtm,
        adjustment_type_descr,
        bk_iroca_hierarchy_node_id,
        adjustment_source_cd,
        discount_flg,
        status_flg,
        sox_tie_in_descr,
        adj_data_update_frequency_cd,
        data_route_descr,
        crtd_by_cisco_worker_party_key,
        upd_by_cisco_worker_party_key,
        create_dtm,
        update_dtm,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        sk_adjustment_type_id_int,
        retained_earnings_flg,
        nrs_transition_flg,
        corporate_revenue_flg,
        dual_gaap_flg
    FROM {{ source('raw', 'n_indrct_rev_cogs_adj_type') }}
),

final AS (
    SELECT
        bk_direct_corp_adj_type_cd,
        bk_adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        effective_start_dtm,
        effective_end_dtm,
        adjustment_type_descr,
        l3_hierarchy_node_type_id,
        l2_hierarchy_node_type_id,
        l1_hierarchy_node_type_id,
        direct_corp_adj_lvl2_descr,
        direct_corp_adj_lvl1_descr,
        adjustment_source_cd,
        discount_flg,
        status_flg,
        sox_tie_in_descr,
        adj_data_update_frequency_cd,
        data_route_descr,
        crtd_by_cisco_worker_party_key,
        upd_by_cisco_worker_party_key,
        direct_indirect_type,
        dual_gaap_flg
    FROM source_n_indrct_rev_cogs_adj_type
)

SELECT * FROM final