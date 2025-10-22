-- Taxa Geral de No-Show (%)
SELECT 
  ROUND(AVG(CASE WHEN "no-show" = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS taxa_cancelamento
FROM refined_beira_mar.clinica_com_clima

-- Impacto do SMS no No-Show
SELECT 
  CASE 
    WHEN f.sms_recebido = 1 THEN 'COM SMS'
    ELSE 'SEM SMS'
  END AS categoria_sms,
  ROUND(AVG(CASE WHEN f.compareceu = 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS taxa_noshow_percentual,
  COUNT(*) AS total_consultas,
  SUM(f.qtd_no_shows) AS total_faltas
FROM star_schema_beira_mar.fato_consultas f
GROUP BY 
  CASE 
    WHEN f.sms_recebido = 1 THEN 'COM SMS'
    ELSE 'SEM SMS'
  END
ORDER BY categoria_sms DESC

-- Impacto da Temperatura em No-Shows (%)

SELECT 
  CASE classificacao_temp
    WHEN 'FRIO' THEN 'Frio'
    WHEN 'AGRADAVEL' THEN 'Agradável'
    WHEN 'QUENTE' THEN 'Quente'
    WHEN 'MUITO_QUENTE' THEN 'Muito Quente'
  END AS categoria_temperatura,
  ROUND(AVG(CASE WHEN "no-show" = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS taxa_noshow
FROM refined_beira_mar.clinica_com_clima
WHERE classificacao_temp IN ('FRIO', 'AGRADAVEL', 'QUENTE', 'MUITO_QUENTE')
GROUP BY classificacao_temp
ORDER BY 
  CASE classificacao_temp
    WHEN 'FRIO' THEN 1
    WHEN 'AGRADAVEL' THEN 2
    WHEN 'QUENTE' THEN 3
    WHEN 'MUITO_QUENTE' THEN 4
  END