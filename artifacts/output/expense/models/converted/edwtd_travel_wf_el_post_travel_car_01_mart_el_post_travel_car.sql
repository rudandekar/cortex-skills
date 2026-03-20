{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_post_travel_car', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_EL_POST_TRAVEL_CAR',
        'target_table': 'EL_POST_TRAVEL_CAR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.595740+00:00'
    }
) }}

WITH 

source_st_post_travel_car AS (
    SELECT
        batch_id,
        tcidte,
        tcmcid,
        tcacbr,
        tcinv,
        tcnpas,
        tcseqn,
        tctype,
        tcacde,
        tcpdte,
        tcploc,
        tccity,
        tcstat,
        tcazip,
        tcpcty,
        tcday,
        tcncar,
        tctcar,
        tcrate,
        tcreas,
        tcccur,
        tcoamt,
        tccfrm,
        tcocur,
        tcnump,
        tccrte,
        tcccom,
        tcddte,
        tcpnr,
        tcctry,
        tctrnr,
        tcmpas,
        tcexrt,
        tcfil1,
        tcfil2,
        fk_travel2,
        xpk_row2,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_post_travel_car') }}
),

final AS (
    SELECT
        tcidte,
        tcmcid,
        tcacbr,
        tcinv,
        tcnpas,
        tcseqn,
        tctype,
        tcacde,
        tcpdte,
        tcploc,
        tccity,
        tcstat,
        tcazip,
        tcpcty,
        tcday,
        tcncar,
        tctcar,
        tcrate,
        tcreas,
        tcccur,
        tcoamt,
        tccfrm,
        tcocur,
        tcnump,
        tccrte,
        tcccom,
        tcddte,
        tcpnr,
        tcctry,
        tctrnr,
        tcmpas,
        tcexrt,
        tcfil1,
        tcfil2,
        fk_travel2,
        xpk_row2
    FROM source_st_post_travel_car
)

SELECT * FROM final