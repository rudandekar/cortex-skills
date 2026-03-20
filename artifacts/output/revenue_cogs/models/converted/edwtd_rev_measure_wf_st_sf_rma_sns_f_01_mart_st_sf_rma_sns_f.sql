{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_sf_rma_sns_f', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_SF_RMA_SNS_F',
        'target_table': 'ST_SF_RMA_SNS_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.397333+00:00'
    }
) }}

WITH 

source_sf_rma_sns_f_ff AS (
    SELECT
        bl_serial_number_key,
        bl_line_key,
        serial_number,
        bl_last_update_date,
        creation_date
    FROM {{ source('raw', 'sf_rma_sns_f_ff') }}
),

transformed_exptrans AS (
    SELECT
    bl_serial_number_key,
    bl_line_key,
    serial_number,
    bl_last_update_date,
    creation_date,
    IFF(LTRIM(RTRIM(BL_SERIAL_NUMBER_KEY)) = '\N',NULL,BL_SERIAL_NUMBER_KEY) AS o_bl_serial_number_key,
    IFF(LTRIM(RTRIM(BL_LINE_KEY)) = '\N',NULL,BL_LINE_KEY) AS o_bl_line_key,
    REG_REPLACE(SERIAL_NUMBER, '[^\w,-. ]', '') AS o_serial_number,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',NULL,TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date,
    IFF(LTRIM(RTRIM(CREATION_DATE)) = '\N',NULL,TO_DATE(CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_creation_date
    FROM source_sf_rma_sns_f_ff
),

final AS (
    SELECT
        bl_serial_number_key,
        bl_line_key,
        serial_number,
        bl_last_update_date,
        creation_date
    FROM transformed_exptrans
)

SELECT * FROM final