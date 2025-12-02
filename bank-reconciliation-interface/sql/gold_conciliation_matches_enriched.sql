-- View: gold_conciliation_matches_enriched
--
-- Junta os matches com informações de valor, descrição, data e dados bancários
-- para facilitar o consumo pela aplicação. Crie a view no Postgres executando
-- este arquivo.

CREATE OR REPLACE VIEW gold_conciliation_matches_enriched AS
SELECT
  m.api_uid,
  m.erp_uid,
  m.stage,
  m.prio,
  m.ddiff,
  a.tenant_id,
  a.bank_code,
  regexp_replace(a.account_number, '\\D', '', 'g') AS acc_tail_raw,
  RIGHT(regexp_replace(a.account_number, '\\D', '', 'g'), 4) AS acc_tail,
  a.date::date AS api_date,
  e.date_br::date AS erp_date,
  COALESCE(a.amount, 0)::float AS api_amount,
  COALESCE(e.amount_client, 0)::float AS erp_amount,
  a.descriptionraw AS api_desc,
  e.description_client AS erp_desc
FROM gold_conciliation_matches m
JOIN silver_api_staging a ON a.id = m.api_uid
JOIN silver_erp_staging e ON e.cd_lancamento = m.erp_uid
WHERE a.tenant_id = e.tenant_id;

