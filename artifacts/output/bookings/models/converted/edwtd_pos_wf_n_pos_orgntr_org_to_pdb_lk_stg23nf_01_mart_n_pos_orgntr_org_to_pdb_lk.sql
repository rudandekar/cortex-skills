{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_orgntr_org_to_pdb_lk_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_POS_ORGNTR_ORG_TO_PDB_LK_STG23NF',
        'target_table': 'N_POS_ORGNTR_ORG_TO_PDB_LK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.227019+00:00'
    }
) }}

WITH 

source_st_wips_disti_pdb_mapping_intf AS (
    SELECT
        disti_profile_id,
        disti_locator_pdb_begeo_id,
        disti_rtm_type
    FROM {{ source('raw', 'st_wips_disti_pdb_mapping_intf') }}
),

final AS (
    SELECT
        bk_wips_originator_id_int,
        bk_be_geo_id_int,
        rtm_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_wips_disti_pdb_mapping_intf
)

SELECT * FROM final