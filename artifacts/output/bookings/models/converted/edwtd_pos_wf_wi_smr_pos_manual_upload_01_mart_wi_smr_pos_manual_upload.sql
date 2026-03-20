{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_smr_pos_manual_upload', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_SMR_POS_MANUAL_UPLOAD',
        'target_table': 'WI_SMR_POS_MANUAL_UPLOAD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.534879+00:00'
    }
) }}

WITH 

source_st_smr_pos_manual_upload AS (
    SELECT
        bk_pos_transcation_id,
        allocation_percentage,
        sales_motion,
        reason_code,
        so_line_id,
        case_number,
        comments,
        upload_id,
        batch_id,
        offer_attribution_id
    FROM {{ source('raw', 'st_smr_pos_manual_upload') }}
),

final AS (
    SELECT
        bk_pos_transaction_id,
        allocation_percentage,
        sales_motion,
        reason_code,
        so_line_id,
        sales_order_line_key,
        bk_so_number_int,
        case_number,
        comments,
        requesting_csco_wrkr_prty_key,
        batch_id,
        offer_attribution_id
    FROM source_st_smr_pos_manual_upload
)

SELECT * FROM final