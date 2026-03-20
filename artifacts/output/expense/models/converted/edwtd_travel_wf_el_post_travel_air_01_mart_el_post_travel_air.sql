{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_post_travel_air', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_EL_POST_TRAVEL_AIR',
        'target_table': 'EL_POST_TRAVEL_AIR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.976111+00:00'
    }
) }}

WITH 

source_st_post_travel_air AS (
    SELECT
        batch_id,
        tfidte,
        tfmcid,
        tfacbr,
        tfinv,
        tfnpas,
        tfseqn,
        tffapt,
        tftapt,
        tfcxr,
        tfflno,
        tfofar,
        tffbc,
        tfccde,
        tfddte,
        tfdtme,
        tfatme,
        tfadte,
        tfconc,
        tffcdi,
        tfccur,
        tfpnr,
        tftrnr,
        tfmpas,
        tfclst,
        tfmile,
        tfexrt,
        tfaorr,
        tffcry,
        tftcry,
        tfsfbt,
        tffil2,
        fk_travel0,
        xpk_row0,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_post_travel_air') }}
),

final AS (
    SELECT
        tfidte,
        tfmcid,
        tfacbr,
        tfinv,
        tfnpas,
        tfseqn,
        tffapt,
        tftapt,
        tfcxr,
        tfflno,
        tfofar,
        tffbc,
        tfccde,
        tfddte,
        tfdtme,
        tfatme,
        tfadte,
        tfconc,
        tffcdi,
        tfccur,
        tfpnr,
        tftrnr,
        tfmpas,
        tfclst,
        tfmile,
        tfexrt,
        tfaorr,
        tffcry,
        tftcry,
        tfsfbt,
        tffil2,
        fk_travel0,
        xpk_row0,
        origin,
        connection_flag,
        seqno_rank
    FROM source_st_post_travel_air
)

SELECT * FROM final