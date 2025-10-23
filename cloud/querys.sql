-- Taxa Geral de No-Show (%)
SELECT 
  ROUND(AVG(CASE WHEN "no-show" = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS taxa_cancelamento
FROM refined_beira_mar.clinica_com_clima


-- Total de Consultas

SELECT 
  SUM(qtd_consultas) AS "Total de Consultas"
FROM star_schema_beira_mar.fato_consultas

-- Tempo Médio de Antecedência (compareceram)

SELECT 
  ROUND(AVG(DATE_DIFF('day', f.data_agendamento_key, f.data_consulta_key)), 1) AS dias_antecedencia_media
FROM star_schema_beira_mar.fato_consultas f
WHERE f.compareceu = 1

-- Tempo Médio de Antecedência (no-show)

SELECT 
  ROUND(AVG(DATE_DIFF('day', f.data_agendamento_key, f.data_consulta_key)), 1) AS dias_antecedencia_noshow
FROM star_schema_beira_mar.fato_consultas f
WHERE f.compareceu = 0


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

  -- Consultas e No-Shows por Dia da Semana

  SELECT 
  d.dia_semana AS x,
  CASE d.dia_semana
    WHEN 1 THEN 'Segunda'
    WHEN 2 THEN 'Terça'
    WHEN 3 THEN 'Quarta'
    WHEN 4 THEN 'Quinta'
    WHEN 5 THEN 'Sexta'
    WHEN 6 THEN 'Sábado'
    WHEN 7 THEN 'Domingo'
  END AS dia_nome,
  SUM(f.qtd_consultas) AS "Total Consultas",
  SUM(f.qtd_no_shows) AS "No-Shows"
FROM star_schema_beira_mar.fato_consultas f
INNER JOIN star_schema_beira_mar.dim_data d 
  ON f.data_consulta_key = d.data_key
GROUP BY d.dia_semana
ORDER BY d.dia_semana


-- Taxa de No-Show por Dia de Antecedência (1-31 dias)
SELECT 
  DATE_DIFF('day', f.data_agendamento_key, f.data_consulta_key) AS dias_antecedencia,
  ROUND(AVG(CASE WHEN f.compareceu = 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS taxa_noshow_pct
FROM star_schema_beira_mar.fato_consultas f
WHERE DATE_DIFF('day', f.data_agendamento_key, f.data_consulta_key) BETWEEN 0 AND 31
GROUP BY DATE_DIFF('day', f.data_agendamento_key, f.data_consulta_key)
ORDER BY dias_antecedencia

-- Taxa de No-Show por Faixa Etária (a cada 10 anos)
SELECT 
  CASE 
    WHEN f.idade < 10 THEN '0-9 anos'
    WHEN f.idade < 20 THEN '10-19 anos'
    WHEN f.idade < 30 THEN '20-29 anos'
    WHEN f.idade < 40 THEN '30-39 anos'
    WHEN f.idade < 50 THEN '40-49 anos'
    WHEN f.idade < 60 THEN '50-59 anos'
    WHEN f.idade < 70 THEN '60-69 anos'
    WHEN f.idade < 80 THEN '70-79 anos'
    ELSE '80+ anos'
  END AS faixa_etaria,
  ROUND(AVG(CASE WHEN f.compareceu = 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS taxa_noshow_pct
FROM star_schema_beira_mar.fato_consultas f
GROUP BY 
  CASE 
    WHEN f.idade < 10 THEN '0-9 anos'
    WHEN f.idade < 20 THEN '10-19 anos'
    WHEN f.idade < 30 THEN '20-29 anos'
    WHEN f.idade < 40 THEN '30-39 anos'
    WHEN f.idade < 50 THEN '40-49 anos'
    WHEN f.idade < 60 THEN '50-59 anos'
    WHEN f.idade < 70 THEN '60-69 anos'
    WHEN f.idade < 80 THEN '70-79 anos'
    ELSE '80+ anos'
  END
ORDER BY 
  MIN(f.idade)