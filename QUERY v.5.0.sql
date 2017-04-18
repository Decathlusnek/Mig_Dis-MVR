----------------------------------------------------------------------------------------------------------------------------------------------
/* 
 Autor: Hugo Bitencourt 
 Descrição: Migração dos relatórios do Oracle Discoverer para o MVReports. Padronização da query (ANSI 92) e remoção dos db_links.
 Modificações:
      12/04/2017: Palavras chave em UPPERCASE. Remoção do db_link @orms. Remoção do db_link @esb_prod. Query não acessa o banco do EBS.
                  Dados provinientes das tabela admin_eul.vm_grupo_fornecedor_ebs foram omitidos. Menor data de pagamento omitida.
      13/04/2017: Alias temporário em todas as colunas da query interna usand o mesmo nome da coluna.
                  Substituição d função NVL para COALESCE.
                  Nomes de tabelas e colunas das tabelas em LOWERCASE.
                  Alias das tabelas com 3 letras, cláusula FROM.
                  Inserção de dados através do usuário na cláusula WHERE para teste (os dados batem com o relatório antigo).
                  Remoção da claúsula ORDER BY.
      17/04/2017  Substituição das junções ds tabelas. Junções usando INNER JOIN ou LEFT/RIGTH OUTER JOIN  ao invés do WHERE.
                  Remoção dos comentários do código da query (remoção do códigos omitidos na query)
                  
*/
----------------------------------------------------------------------------------------------------------------------------------------------

SELECT qry.sup_name,
       qry.location_id,
       qry.wh_name,
       qry.fiscal_doc_no,
       qry.series_no,
       qry.subseries_no,
       qry.cfop,
       qry.cfop_desc,
       qry.utilization_id,
       qry.utilization_desc,
       qry.status_nf,
       qry.total_serv_value,
       qry.total_item_value,
       qry.total_doc_value,
       qry.issue_date,
       qry.entry_or_exit_date,
       qry.item,
       qry.item_comments,
       qry.po_number,
       qry.buyer_name,
       qry.dept_name,
       qry.quantity,
       qry.UNIT_COST,
       qry.total_cost,
       qry.total_calc_cost,
       qry.other_cost,
       qry.freight_cost,
       qry.discount_value,
       qry.unit_cost_with_disc,
       qry.item_desc,
       qry.item_desc_secondary,
       qry.vlr_icmsst_total,
       qry.vlr_icmsst_item,
       qry.vlr_ipi_total,
       qry.vlr_ipi_item,
       qry.vlr_cofins_total,
       qry.vlr_cofins_item,
       qry.vlr_icms_total,
       qry.vlr_icms_item,
       qry.vlr_icms_item,
       qry.vlr_pis_total,
       qry.vlr_pis_item,
       qry.partner_desc,
       qry.group_name,
       qry.comment_desc_pedido, 
       qry.total_discount_value,
       qry.schedule_date,
       qry.accounting_date,
       qry.last_update_datetime
FROM ( SELECT 
       prt.partner_id,
       prt.partner_desc,
       sup.supplier, 
       sup.sup_name,
       COALESCE (COALESCE(
                  (SELECT TO_CHAR(COALESCE(nsa.perc_vpc_exp,0),'000D00')||TO_CHAR(COALESCE(nsa.perc_ve_exp,0),'000D00')||TO_CHAR(COALESCE(nsa.perc_eq_exp,0),'000D00') perc_vpc_exp
                  FROM rmsprd.nb_sup_attr_vpc nsa--ll
                  WHERE nsa.supplier = sup.supplier
                      AND nsa.item = itm.item
                      AND ROWNUM = 1
               ),
        ( SELECT TO_CHAR(COALESCE(nsa.perc_vpc_exp,0),'000D00')||TO_CHAR(COALESCE (nsa.perc_ve_exp,0),'000D00')||TO_CHAR(COALESCE (nsa.perc_eq_exp,0),'000D00') perc_vpc_exp
          FROM rmsprd.nb_sup_attr_vpc nsa --ll
          WHERE nsa.supplier = sup.supplier
              AND nsa.group_no = dep.group_no
              AND ROWNUM = 1)
        ),
       TO_CHAR(COALESCE(sat.nb_perc_vpc,0),'000D00')||TO_CHAR(COALESCE(sat.nb_perc_ve,0),'000D00')||TO_CHAR(COALESCE(sat.nb_perc_eq,0),'000D00')) nb_perc_vpc_supp,
       sat.nb_base_vlr AS nb_base_vlr_supp, 
       sat.nb_payment_form AS nb_payment_form_supp,
       nb_re_orms_discoverer.f_code_detail('FMRT',fdh.requisition_type) AS tipo_requisicao,
       fdh.fiscal_doc_id AS fiscal_doc_id,
       fdd.fiscal_doc_line_id AS fiscal_doc_line_id,
       fdt.type_id AS type_id,
       fdt.type_desc AS type_desc,
       fdh.location_id AS location_id,
       fdh.location_type AS location_type,
       wh.wh_name AS wh_name,
       fdh.fiscal_doc_no AS fiscal_doc_no,
       fdh.series_no AS series_no,
       fdh.subseries_no AS subseries_no,       
       fuc.cfop AS cfop,
       fcp.cfop_desc AS cfop_desc,
       ffu.utilization_id AS utilization_id,
       ffu.utilization_desc AS utilization_desc,
       fdh.status AS codigo_status_nf,
       nb_re_orms_discoverer.f_code_detail('FMSD', fdh.status) AS status_nf,
       fsc.mode_type AS mode_type,
       fsc.schedule_date AS schedule_date,
       fsc.accounting_date AS accounting_date,
       fsc.last_update_datetime AS last_update_datetime,
       nb_re_orms_discoverer.f_code_detail('FMMO',fsc.mode_type) AS modo_nf,
       fdh.total_serv_value AS total_serv_value, 
       fdh.total_item_value AS total_item_value,
       fdh.total_doc_value AS total_doc_value,
       fdh.total_discount_value AS total_discount_value,
       trunc(fdh.issue_date) AS issue_date,
       trunc(fdh.entry_or_exit_date) AS entry_or_exit_date,
       fdd.item AS item,
       fdd.classification_id AS classification_id,
       substr(itm.comments,1,15) AS item_comments,
       fdd.requisition_no AS po_number,       
       ord.contract_no AS contract_no,
       ord.buyer AS buyer,
       ord.buyer_name AS buyer_name,
       ord.terms_code AS terms_code,
       ord.terms_desc AS terms_desc,
       ord.orig_approval_date AS orig_approval_date,
       ord.written_date AS written_date, 
       ord.comment_desc AS comment_desc_pedido,      
       COALESCE(ord.vlr_total_pedido_sem_ipi,0) AS vlr_total_pedido_sem_ipi,
       dep.group_no AS group_no,
       grp.group_name AS group_name,
       itm.dept AS dept,       
       dep.dept_name AS dept_name,
       fdd.quantity AS quantity,
       fdd.unit_cost AS unit_cost,
       fdd.unit_cost AS unit_cost_detalhado,
       fdd.total_cost AS total_cost,
       fdd.total_calc_cost AS total_calc_cost,
       COALESCE(fdd.other_cost,0) AS other_cost,
       COALESCE(fdd.freight_cost,0) AS freight_cost,
       COALESCE(fdh.insurance_cost,0) AS insurance_cost,
       fdd.discount_type AS discount_type,
       nb_re_orms_discoverer.f_code_detail('FMVP', fdd.discount_type) AS discount_type_desc,
       COALESCE(fdd.discount_value,0) AS discount_value,
       fdd.unit_cost_with_disc AS unit_cost_with_disc,
       itm.item_desc AS item_desc,
       itm.item_desc_secondary AS item_desc_secondary,
       nb_re_orms_discoverer.f_tax_doc_header('ICMSST',fdh.fiscal_doc_id) AS vlr_icmsst_total,
       nb_re_orms_discoverer.f_tax_doc_header('IPI',fdh.fiscal_doc_id) AS vlr_ipi_total,
       nb_re_orms_discoverer.f_tax_doc_header('COFINS',fdh.fiscal_doc_id) AS vlr_cofins_total,
       nb_re_orms_discoverer.f_tax_doc_header('ICMS',fdh.fiscal_doc_id) AS vlr_icms_total,
       nb_re_orms_discoverer.f_tax_doc_header('PIS',fdh.fiscal_doc_id) AS vlr_pis_total,       
       nb_re_orms_discoverer.f_tax_doc_detail_header('ICMSST',fdd.fiscal_doc_line_id) AS vlr_icmsst_item,
       nb_re_orms_discoverer.f_tax_doc_detail_header('IPI',fdd.fiscal_doc_line_id) AS vlr_ipi_item,
       nb_re_orms_discoverer.f_tax_doc_detail_header('COFINS',fdd.fiscal_doc_line_id) AS vlr_cofins_item,
       nb_re_orms_discoverer.f_tax_doc_detail_header('ICMS',fdd.fiscal_doc_line_id) AS vlr_icms_item,
       nb_re_orms_discoverer.f_tax_doc_detail_header('PIS', fdd.fiscal_doc_line_id) AS vlr_pis_item,
       (  SELECT SUM(fct.total_value)      
          FROM rmsprd.fm_fiscal_doc_complement fdc --d3,
          INNER JOIN rmsprd.fm_fiscal_doc_header fdh ON fdc.compl_fiscal_doc_id = fdh.fiscal_doc_id
          INNER JOIN rmsprd.fm_fiscal_doc_detail fdd ON fdh.fiscal_doc_id = fdd.fiscal_doc_id
          INNER JOIN rmsprd.fm_fiscal_doc_tax_detail fct ON fdd.fiscal_doc_line_id = fct.fiscal_doc_line_id
          INNER JOIN rmsprd.fm_utilization_cfop fuc ON fdh.utilization_cfop = fuc.utilization_cfop
          WHERE NOT EXISTS ( SELECT 1
                             FROM rmsprd.nb_fm_utilization_attributes nfua
                             WHERE nfua.variable = 'NB_CTRC_UTILIZATION'
                                   AND nfua.string_value = 'Y'
                                  AND nfua.utilization_id = fuc.utilization_id
                              )
                AND fdc.fiscal_doc_id = fdh.fiscal_doc_id
                AND fdd.item = fdd.item
                AND fct.vat_code = 'ICMS'
                AND fdh.schedule_no = fdh.schedule_no
          ) vlr_icms_item_nf_complementar,
       fdd.requisition_no AS ol_requisition_no, 
       fdd.location_id AS ol_location_id,
       fdd.item AS ol_item
       FROM fm_fiscal_doc_header   fdh
       INNER JOIN fm_fiscal_doc_detail  fdd ON fdh.fiscal_doc_id = fdd.fiscal_doc_id
       INNER JOIN fm_fiscal_doc_type fdt    ON fdh.type_id = fdt.type_id
       INNER JOIN fm_utilization_cfop fuc   ON fdh.utilization_cfop = fuc.utilization_cfop   
       INNER JOIN fm_fiscal_utilization ffu ON fuc.utilization_id = ffu.utilization_id 
       INNER JOIN fm_cfop fcp               ON fuc.cfop = fcp.cfop
       INNER JOIN fm_schedule fsc           ON fdh.schedule_no = fsc.schedule_no
       INNER JOIN wh wh                     ON fdh.location_id = wh.wh
       INNER JOIN item_master itm           ON fdd.item = itm.item
       INNER JOIN deps dep                  ON itm.dept = dep.dept
       INNER JOIN groups grp                ON dep.group_no = grp.group_no
       LEFT OUTER JOIN ( SELECT odh.order_no AS order_no,
                                 odh.contract_no AS contract_no,
                                 odh.orig_approval_date AS orig_approval_date,
                                 odh.written_date AS written_date, 
                                 teh.terms_code AS terms_code,
                                 teh.terms_desc AS terms_desc,
                                 buy.buyer AS buyer,
                                 buy.buyer_name AS buyer_name,
                                 odh.comment_desc AS comment_desc,
                                 SUM(orl.qty_ordered * orl.unit_cost) AS vlr_total_pedido_sem_ipi
                           FROM ordhead     odh
                           INNER JOIN terms_head teh ON odh.terms = teh.terms
                           INNER JOIN ordloc orl ON  odh.order_no = orl.order_no
                           LEFT OUTER JOIN buyer buy ON odh.buyer = buy.buyer
                           GROUP BY odh.order_no,
                                    odh.contract_no,
                                    odh.orig_approval_date,
                                    odh.written_date,
                                    teh.terms_code,
                                    teh.terms_desc,
                                    buy.buyer,
                                    buy.buyer_name,
                                    odh.comment_desc
                        ) ord               ON fdd.requisition_no = ord.order_no
       INNER JOIN sups sup                  ON TO_NUMBER(fdh.key_value_1) = sup.supplier 
       INNER JOIN sup_import_attr sia       ON sup.supplier = sia.supplier
       LEFT OUTER JOIN sup_attributes sat   ON sup.supplier = sat.supplier
       INNER JOIN partner prt               ON (sia.partner_1 = prt.partner_id)  AND (sia.partner_type_1 = prt.partner_type)    
       WHERE fsc.mode_type = 'ENT' 
             AND fdh.requisition_type = 'PO'
             AND fdh.location_type = 'W'
             AND fdh.module = 'SUPP'
             AND fdh.status not in ('E','I','W')
             AND fsc.status = 'C'
        )qry
WHERE qry.ITEM IN ('220002')
      AND qry.ACCOUNTING_DATE >= TO_DATE('01/07/10','DD/MM,YY') AND qry.ACCOUNTING_DATE < TO_DATE('14/09/17','DD/MM/YY')
      --AND qry.PO_NUMBER = 496999
      AND qry.CODIGO_STATUS_NF IN ('C','A')
      --AND qry.FISCAL_DOC_NO IN ('181179')
      AND qry.GROUP_NAME IN ('01 MOVEIS_')
      AND qry.PARTNER_DESC IN ('WG- ARAPLAC IND E COM DE MOVEIS LTDA 77.215.606')
      AND qry.ITEM_DESC IN ('GR 10P3G VIENA 1849061N 184L TB/GF ARAPLAC')
      AND qry.BUYER_NAME IS NULL --IN ('RENAN DRAGO') 
      AND qry.SUP_NAME IN ('RNV- ARAPLAC IND E COM DE MOVEIS LTDA 77.215.606/0001-79')
      AND qry.UTILIZATION_DESC IN ('Entrada de Armazém Geral - FATURA - SEM destaque ICMS')
      AND qry.WH_NAME IN ('CD 47 - RNV Revenda')
      AND qry.ENTRY_OR_EXIT_DATE BETWEEN TO_DATE('20/07/10','DD/MM/YY') AND TO_DATE('17/12/17','DD/MM/YY')
      AND qry.ISSUE_DATE BETWEEN TO_DATE('13/07/10','DD/MM/YY') AND TO_DATE('17/12/17','DD/MM/YY');
 