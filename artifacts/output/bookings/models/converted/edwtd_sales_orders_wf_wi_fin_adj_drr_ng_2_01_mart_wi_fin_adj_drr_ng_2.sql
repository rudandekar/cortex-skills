{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_fin_adj_drr_ng_2', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_FIN_ADJ_DRR_NG_2',
        'target_table': 'WI_FIN_ADJ_DRR_NG_2',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.960120+00:00'
    }
) }}

WITH 

source_wi_fin_adj_drr_ng_1 AS (
    SELECT
        restatement_sub_type,
        book_source,
        book_source_sub_type,
        order_number,
        order_line_id,
        sales_territory_name_code,
        restated_sales_terr_name_code,
        party_id,
        sav_id,
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
        status,
        validation_comments,
        trx_id_int,
        file_num_int,
        deal_id,
        new_agent_num,
        svc_ind,
        quarter_id,
        product_id
    FROM {{ source('raw', 'wi_fin_adj_drr_ng_1') }}
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
        sk_customer_trx_line_id_int,
        bk_top_pos_trx_id_int,
        bk_pos_transaction_id_int,
        bk_sales_adj_line_number_int,
        sk_trx_id_int,
        sk_trx_distri_id_int,
        status,
        validation_comments,
        file_num_int,
        restatement_type_code,
        deal_id,
        new_agent_num,
        svc_ind,
        quarter_id,
        product_id
    FROM source_wi_fin_adj_drr_ng_1
)

SELECT * FROM final