{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_fin_adj_drr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_FIN_ADJ_DRR',
        'target_table': 'WI_FIN_ADJ_DRR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.399554+00:00'
    }
) }}

WITH 

source_st_fin_adj_drr_upload AS (
    SELECT
        restatement_sub_type,
        book_source,
        source_system,
        order_number,
        order_line_id,
        sales_territory_name_code,
        restated_sales_terr_name_code,
        sav_id,
        party_id,
        split,
        dnr_flag,
        effective_date,
        expiry_date,
        approved_by,
        approved_dtm,
        uploaded_by,
        uploaded_dtm,
        file_name,
        reason_descr,
        trx_id_int,
        batch_id,
        deal_id,
        new_agent_num,
        svc_ind,
        quarter_id,
        product_id
    FROM {{ source('raw', 'st_fin_adj_drr_upload') }}
),

final AS (
    SELECT
        restatement_sub_type,
        book_source,
        book_source_sub_type,
        order_number,
        order_line_id,
        sales_territory_name_code,
        derived_party_id,
        derived_sav_id,
        restated_sales_terr_name_code,
        party_id,
        sav_id,
        split,
        dnr_flag,
        adjustment_id,
        effective_date,
        expiry_date,
        approved_by,
        approved_dtm,
        uploaded_by,
        uploaded_dtm,
        file_name,
        reason_descr,
        trx_id_int,
        file_validation,
        batch_id,
        deal_id,
        new_agent_num,
        svc_ind,
        quarter_id,
        product_id
    FROM source_st_fin_adj_drr_upload
)

SELECT * FROM final