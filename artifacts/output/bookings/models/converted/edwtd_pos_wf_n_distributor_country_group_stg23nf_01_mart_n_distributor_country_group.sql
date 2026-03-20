{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_distributor_country_group_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTRIBUTOR_COUNTRY_GROUP_STG23NF',
        'target_table': 'N_DISTRIBUTOR_COUNTRY_GROUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.464272+00:00'
    }
) }}

WITH 

source_st_tbl_country_grouping AS (
    SELECT
        country,
        country_grouping_ecc,
        country_grouping,
        country_sub_group
    FROM {{ source('raw', 'st_tbl_country_grouping') }}
),

final AS (
    SELECT
        bk_iso_country_cd,
        country_ranking_grp_cd,
        country_rgnl_grp_name,
        country_rgnl_subgrp_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_tbl_country_grouping
)

SELECT * FROM final