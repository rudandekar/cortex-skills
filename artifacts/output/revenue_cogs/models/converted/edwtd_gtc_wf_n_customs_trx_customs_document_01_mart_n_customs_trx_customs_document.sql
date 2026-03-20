{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_customs_trx_customs_document', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_CUSTOMS_TRX_CUSTOMS_DOCUMENT',
        'target_table': 'N_CUSTOMS_TRX_CUSTOMS_DOCUMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.741890+00:00'
    }
) }}

WITH 

source_n_customs_trx_customs_document AS (
    SELECT
        bk_customs_document_type_cd,
        customs_trx_key,
        freight_payment_method_cd,
        location_type_cd,
        duty_cd,
        ship_to_flg,
        leg_1_bill_cst_gtc_biz_enty_nm,
        cmrcl_inv_slr_gtc_biz_ent_nm,
        leg_2_bill_cst_gtc_biz_enty_nm,
        leg_2_ship_cst_gtc_biz_enty_nm,
        leg_1_ship_cst_gtc_biz_enty_nm,
        inco_term_cd,
        ship_erp_cust_acct_loc_use_key,
        bill_erp_cust_acct_loc_use_key,
        importer_of_record_addr,
        importer_of_record_name,
        importer_of_record_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        src_doc_num_int
    FROM {{ source('raw', 'n_customs_trx_customs_document') }}
),

final AS (
    SELECT
        bk_customs_document_type_cd,
        customs_trx_key,
        freight_payment_method_cd,
        location_type_cd,
        duty_cd,
        ship_to_flg,
        leg_1_bill_cst_gtc_biz_enty_nm,
        cmrcl_inv_slr_gtc_biz_ent_nm,
        leg_2_bill_cst_gtc_biz_enty_nm,
        leg_2_ship_cst_gtc_biz_enty_nm,
        leg_1_ship_cst_gtc_biz_enty_nm,
        inco_term_cd,
        ship_erp_cust_acct_loc_use_key,
        bill_erp_cust_acct_loc_use_key,
        importer_of_record_addr,
        importer_of_record_name,
        importer_of_record_num,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        src_doc_num_int
    FROM source_n_customs_trx_customs_document
)

SELECT * FROM final