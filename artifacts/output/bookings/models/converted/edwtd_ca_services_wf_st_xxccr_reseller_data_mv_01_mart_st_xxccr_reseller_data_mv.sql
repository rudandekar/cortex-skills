{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxccr_reseller_data_mv', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_XXCCR_RESELLER_DATA_MV',
        'target_table': 'ST_XXCCR_RESELLER_DATA_MV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.781927+00:00'
    }
) }}

WITH 

source_ff_xxccr_reseller_data_mv AS (
    SELECT
        batchid,
        cr_party_id,
        reseller_id,
        disti_id,
        last_reported_pos_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'ff_xxccr_reseller_data_mv') }}
),

transformed_exp_reseller_data_clng AS (
    SELECT
    batchid,
    cr_party_id,
    reseller_id,
    disti_id,
    last_reported_pos_date,
    action_code,
    create_datetime,
    ltrim(rtrim(CR_PARTY_ID)) AS cr_party_id1,
    ltrim(rtrim(RESELLER_ID)) AS reseller_id1,
    to_date(To_Char(LAST_REPORTED_POS_DATE),'MM/DD/YYYY' ) AS last_reported_pos_date1
    FROM source_ff_xxccr_reseller_data_mv
),

final AS (
    SELECT
        batchid,
        cr_party_id,
        reseller_id,
        disti_id,
        last_reported_pos_date,
        action_code,
        create_datetime
    FROM transformed_exp_reseller_data_clng
)

SELECT * FROM final