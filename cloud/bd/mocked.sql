-- ============================================================================
-- BEIRA-MAR - MASSA DE DADOS MOCKADOS PARA RELATÓRIO DE BI
-- ============================================================================
-- Data: Novembro 2025 (mês fechado para relatório mensal)
-- 
-- HISTÓRIA DOS DADOS:
-- 1. Segunda e Terça são os dias mais fracos → Oportunidade para promoção
-- 2. Design Simples tem alta demanda mas alto cancelamento (~28%)
-- 3. Hidrolipo NA tem baixo cancelamento (~10%) - clientes premium comprometidos
-- 4. Início do mês (semanas 1-2) mais movimentado (após recebimento de salário)
-- 5. Quinta é o melhor dia para agendamentos importantes
-- 6. Alguns clientes VIP recorrentes
-- 7. Faixa etária 20-35 com mais cancelamentos
-- ============================================================================

DROP DATABASE IF EXISTS BeiraMar;
CREATE DATABASE BeiraMar;
USE BeiraMar;

-- ============================================================================
-- ESTRUTURA DAS TABELAS
-- ============================================================================

CREATE TABLE cargo (
    id_cargo INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(255)
);

CREATE TABLE files_entity (
    id INT PRIMARY KEY AUTO_INCREMENT,
    content_type VARCHAR(200),
    created_at DATE,
    original_name VARCHAR(200),
    size DOUBLE,
    stored_name VARCHAR(200)
);

CREATE TABLE usuario (
    id_usuario INT PRIMARY KEY AUTO_INCREMENT,
    foto_perfil_id INT,
    nome VARCHAR(100),
    email VARCHAR(80),
    telefone VARCHAR(45),
    senha VARCHAR(300),
    dt_nasc DATE,
    fk_cargo INT,
    CONSTRAINT fkUsuarioCargo FOREIGN KEY (fk_cargo) REFERENCES cargo (id_cargo),
    CONSTRAINT fkUsuarioFotoPerfil FOREIGN KEY (foto_perfil_id) REFERENCES files_entity(id)
);

CREATE TABLE logSenha (
    id_logSenha INT AUTO_INCREMENT,
    fk_usuario INT,
    token CHAR(6),
    dataLog DATETIME,
    status VARCHAR(45),
    PRIMARY KEY (id_logSenha, fk_usuario),
    CONSTRAINT fkUsuarioLog FOREIGN KEY (fk_usuario) REFERENCES usuario (id_usuario)
);

CREATE TABLE disponibilidade_entity (
    id_disponibilidade INT AUTO_INCREMENT,
    fk_funcionario INT,
    dia_semana VARCHAR(45),
    hora_inicio TIME,
    hora_fim TIME,
    dia_mes VARCHAR(45),
    fk_disponibilidade_excecao INT,
    PRIMARY KEY(id_disponibilidade, fk_funcionario),
    CONSTRAINT fkDispDisp FOREIGN KEY (fk_disponibilidade_excecao) REFERENCES disponibilidade_entity (id_disponibilidade)
);

CREATE TABLE pacote (
    id_pacote INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100),
    preco_total_sem_desconto DECIMAL(10,2),
    qtd_sessoes_total INT,
    tempo_limite_dias INT
);

CREATE TABLE valorPacoteComDesconto (
    fk_usuario INT,
    fk_pacote INT,
    valorTotal DECIMAL(10,2),
    PRIMARY KEY (fk_usuario, fk_pacote),
    CONSTRAINT fkValorUsuario FOREIGN KEY (fk_usuario) REFERENCES usuario (id_usuario),
    CONSTRAINT fkValorPacote FOREIGN KEY (fk_pacote) REFERENCES pacote (id_pacote)
);

CREATE TABLE servico (
    id_servico INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100),
    duracao INT,
    descricao VARCHAR(255),
    preco DECIMAL(10,2)
);

CREATE TABLE sessoes_pacote (
    id_sessoes_pacote INT AUTO_INCREMENT,
    fk_pacote INT,
    fk_servico INT,
    qtd_sessoes INT,
    PRIMARY KEY (id_sessoes_pacote, fk_pacote, fk_servico),
    CONSTRAINT fkSessoesPacote FOREIGN KEY (fk_pacote) REFERENCES pacote (id_pacote),
    CONSTRAINT fkSessoesServico FOREIGN KEY (fk_servico) REFERENCES servico (id_servico)
);

CREATE TABLE agendamento (
    id_agendamento INT AUTO_INCREMENT,
    fk_servico INT,
    fk_cliente INT,
    fk_funcionario INT,
    dt_hora DATETIME,
    valor_pago DECIMAL(10,2),
    status_agendamento ENUM('Agendado', 'Concluido', 'Cancelado'),
    status ENUM('Agendado', 'Concluido', 'Cancelado'),
    dt_validade DATE,
    fk_pacote INT,
    PRIMARY KEY(id_agendamento, fk_servico, fk_cliente, fk_funcionario),
    CONSTRAINT fkAgendamentoServico FOREIGN KEY (fk_servico) REFERENCES servico (id_servico),
    CONSTRAINT fkAgendamentoCliente FOREIGN KEY (fk_cliente) REFERENCES usuario (id_usuario),
    CONSTRAINT fkAgendamentoFuncionario FOREIGN KEY (fk_funcionario) REFERENCES usuario (id_usuario),
    CONSTRAINT fkAgendamentoPacote FOREIGN KEY (fk_pacote) REFERENCES pacote (id_pacote)
);

CREATE TABLE logStatus (
    fk_agendamento INT PRIMARY KEY,
    status ENUM('Agendado', 'Concluido', 'Cancelado'),
    dtAlteracao DATETIME,
    CONSTRAINT fkLogAgendamento FOREIGN KEY (fk_agendamento) REFERENCES agendamento (id_agendamento)
);

-- ============================================================================
-- INSERÇÃO DE DADOS BASE
-- ============================================================================

-- Cargos
INSERT INTO cargo (nome) VALUES 
    ('Administrador'),
    ('Funcionário'),
    ('Cliente');

-- Serviços da Clínica (conforme documentação oficial)
INSERT INTO servico (nome, duracao, descricao, preco) VALUES
    ('Massagem Modeladora', 40, 'Técnica para modelar o corpo e reduzir medidas', 90.00),
    ('Drenagem Linfática', 60, 'Massagem que estimula o sistema linfático', 100.00),
    ('Hidrolipo NA', 150, 'Procedimento para redução de gordura localizada', 180.00),
    ('Massagem Relaxante', 60, 'Massagem suave que alivia tensões musculares', 100.00),
    ('Aplicação de Enzimas', 60, 'Tratamento para redução de gordura localizada', 180.00),
    ('Limpeza de Pele', 120, 'Procedimento completo para higienização profunda', 150.00),
    ('Design de Sobrancelhas com Henna', 90, 'Design e coloração das sobrancelhas', 45.00),
    ('Design Simples de Sobrancelhas', 30, 'Modelagem rápida das sobrancelhas', 30.00),
    ('Depilação Facial', 30, 'Depilação facial suave e eficaz', 35.00),
    ('Detox Corporal', 120, 'Tratamento de desintoxicação do corpo', 150.00),
    ('Pump Up Glúteos + Eletroestimulação', 60, 'Massagens e eletroestimulação para glúteos', 90.00);

-- Pacotes Promocionais
INSERT INTO pacote (nome, preco_total_sem_desconto, qtd_sessoes_total, tempo_limite_dias) VALUES
    ('Pacote Corpo Perfeito', 450.00, 5, 60),
    ('Pacote Relaxamento Total', 300.00, 3, 45),
    ('Pacote Noiva', 600.00, 6, 90),
    ('Pacote Beleza Express', 150.00, 4, 30);

-- Sessões dos Pacotes
INSERT INTO sessoes_pacote (fk_pacote, fk_servico, qtd_sessoes) VALUES
    (1, 1, 3),  -- Corpo Perfeito: 3x Massagem Modeladora
    (1, 2, 2),  -- Corpo Perfeito: 2x Drenagem
    (2, 4, 3),  -- Relaxamento: 3x Massagem Relaxante
    (3, 6, 2),  -- Noiva: 2x Limpeza de Pele
    (3, 7, 2),  -- Noiva: 2x Design com Henna
    (3, 2, 2),  -- Noiva: 2x Drenagem
    (4, 8, 2),  -- Beleza Express: 2x Design Simples
    (4, 9, 2);  -- Beleza Express: 2x Depilação Facial

-- ============================================================================
-- USUÁRIOS - FUNCIONÁRIA E CLIENTES
-- ============================================================================

-- Ana Carla (Proprietária/Funcionária) - ID 1
INSERT INTO usuario (nome, email, telefone, senha, dt_nasc, fk_cargo) VALUES
    ('Ana Carla Silva', 'anacarla@beiramar.com', '11989407498', '$2a$10$hash_seguro', '2002-03-15', 1);

-- Clientes com perfis variados (IDs 2-51)
-- Faixa etária diversificada para análise
INSERT INTO usuario (nome, email, telefone, senha, dt_nasc, fk_cargo) VALUES
    -- Clientes VIP (frequentes) - IDs 2-6
    ('Fernanda Oliveira', 'fernanda.oli@email.com', '11987654321', '$2a$10$hash', '1990-05-20', 3),
    ('Juliana Costa', 'ju.costa@email.com', '11976543210', '$2a$10$hash', '1985-08-12', 3),
    ('Mariana Santos', 'mari.santos@email.com', '11965432109', '$2a$10$hash', '1992-11-03', 3),
    ('Patricia Lima', 'pati.lima@email.com', '11954321098', '$2a$10$hash', '1988-02-28', 3),
    ('Camila Rodrigues', 'camila.rod@email.com', '11943210987', '$2a$10$hash', '1995-07-15', 3),
    
    -- Clientes regulares - IDs 7-20
    ('Beatriz Almeida', 'bia.almeida@email.com', '11932109876', '$2a$10$hash', '1998-04-10', 3),
    ('Carolina Ferreira', 'carol.fer@email.com', '11921098765', '$2a$10$hash', '2000-09-25', 3),
    ('Daniela Souza', 'dani.souza@email.com', '11910987654', '$2a$10$hash', '1993-12-08', 3),
    ('Elena Martins', 'elena.m@email.com', '11909876543', '$2a$10$hash', '1987-06-30', 3),
    ('Flávia Pereira', 'flavia.p@email.com', '11898765432', '$2a$10$hash', '1996-01-18', 3),
    ('Gabriela Nunes', 'gabi.nunes@email.com', '11887654321', '$2a$10$hash', '1999-03-22', 3),
    ('Helena Barbosa', 'helena.b@email.com', '11876543210', '$2a$10$hash', '1991-10-14', 3),
    ('Isabela Cardoso', 'isa.cardoso@email.com', '11865432109', '$2a$10$hash', '1994-07-07', 3),
    ('Joana Ribeiro', 'joana.rib@email.com', '11854321098', '$2a$10$hash', '1989-05-02', 3),
    ('Kelly Mendes', 'kelly.m@email.com', '11843210987', '$2a$10$hash', '1997-08-19', 3),
    ('Larissa Castro', 'lari.castro@email.com', '11832109876', '$2a$10$hash', '2001-02-14', 3),
    ('Mônica Dias', 'monica.d@email.com', '11821098765', '$2a$10$hash', '1986-11-28', 3),
    ('Natália Gomes', 'nati.gomes@email.com', '11810987654', '$2a$10$hash', '1993-04-05', 3),
    ('Olívia Ramos', 'olivia.r@email.com', '11809876543', '$2a$10$hash', '1990-09-12', 3),
    ('Paula Teixeira', 'paula.t@email.com', '11798765432', '$2a$10$hash', '1988-06-25', 3),
    
    -- Clientes jovens (mais propensas a cancelar) - IDs 21-30
    ('Rafaela Moura', 'rafa.moura@email.com', '11787654321', '$2a$10$hash', '2002-01-08', 3),
    ('Sabrina Lopes', 'sabrina.l@email.com', '11776543210', '$2a$10$hash', '2003-05-30', 3),
    ('Tatiane Silva', 'tati.silva@email.com', '11765432109', '$2a$10$hash', '2001-08-17', 3),
    ('Úrsula Vieira', 'ursula.v@email.com', '11754321098', '$2a$10$hash', '2000-12-03', 3),
    ('Vanessa Correia', 'vanessa.c@email.com', '11743210987', '$2a$10$hash', '2002-03-21', 3),
    ('Wanda Freitas', 'wanda.f@email.com', '11732109876', '$2a$10$hash', '2001-07-14', 3),
    ('Ximena Araújo', 'ximena.a@email.com', '11721098765', '$2a$10$hash', '2003-10-29', 3),
    ('Yasmin Borges', 'yasmin.b@email.com', '11710987654', '$2a$10$hash', '2000-04-06', 3),
    ('Zélia Fonseca', 'zelia.f@email.com', '11709876543', '$2a$10$hash', '2002-09-18', 3),
    ('Amanda Pinto', 'amanda.p@email.com', '11698765432', '$2a$10$hash', '2001-11-11', 3),
    
    -- Clientes 40+ (menos cancelamentos) - IDs 31-40
    ('Berenice Andrade', 'berenice.a@email.com', '11687654321', '$2a$10$hash', '1975-03-15', 3),
    ('Célia Machado', 'celia.m@email.com', '11676543210', '$2a$10$hash', '1970-08-22', 3),
    ('Dolores Cunha', 'dolores.c@email.com', '11665432109', '$2a$10$hash', '1978-01-30', 3),
    ('Edna Monteiro', 'edna.m@email.com', '11654321098', '$2a$10$hash', '1972-06-18', 3),
    ('Fátima Nascimento', 'fatima.n@email.com', '11643210987', '$2a$10$hash', '1968-12-05', 3),
    ('Glória Campos', 'gloria.c@email.com', '11632109876', '$2a$10$hash', '1973-04-28', 3),
    ('Hilda Rocha', 'hilda.r@email.com', '11621098765', '$2a$10$hash', '1965-09-10', 3),
    ('Irene Xavier', 'irene.x@email.com', '11610987654', '$2a$10$hash', '1976-02-23', 3),
    ('Júlia Medeiros', 'julia.med@email.com', '11609876543', '$2a$10$hash', '1971-07-07', 3),
    ('Lourdes Azevedo', 'lourdes.a@email.com', '11598765432', '$2a$10$hash', '1969-10-15', 3),
    
    -- Clientes ocasionais - IDs 41-50
    ('Marta Duarte', 'marta.d@email.com', '11587654321', '$2a$10$hash', '1995-05-12', 3),
    ('Neusa Barros', 'neusa.b@email.com', '11576543210', '$2a$10$hash', '1982-08-28', 3),
    ('Odete Farias', 'odete.f@email.com', '11565432109', '$2a$10$hash', '1990-11-03', 3),
    ('Priscila Guerra', 'priscila.g@email.com', '11554321098', '$2a$10$hash', '1997-02-18', 3),
    ('Queila Silveira', 'queila.s@email.com', '11543210987', '$2a$10$hash', '1984-06-25', 3),
    ('Rosa Marques', 'rosa.m@email.com', '11532109876', '$2a$10$hash', '1992-09-08', 3),
    ('Sandra Viana', 'sandra.v@email.com', '11521098765', '$2a$10$hash', '1986-12-21', 3),
    ('Tereza Carvalho', 'tereza.c@email.com', '11510987654', '$2a$10$hash', '1993-03-14', 3),
    ('Valéria Moreira', 'valeria.m@email.com', '11509876543', '$2a$10$hash', '1988-07-30', 3),
    ('Wilma Nogueira', 'wilma.n@email.com', '11498765432', '$2a$10$hash', '1991-10-05', 3);

-- ============================================================================
-- AGENDAMENTOS DE NOVEMBRO 2025
-- ============================================================================
-- HISTÓRIA:
-- - Semana 1 (01-08): Alta demanda (início do mês, pós-salário)
-- - Semana 2 (09-15): Demanda moderada
-- - Semana 3 (16-22): Queda leve (meio do mês)
-- - Semana 4 (23-30): Recuperação (Black Friday + fim de mês)
-- 
-- PADRÕES POR DIA:
-- - Segunda/Terça: Baixa demanda (oportunidade de promoção)
-- - Quarta/Quinta: Alta demanda
-- - Sexta: Demanda moderada-alta
-- - Sábado: Alta demanda, mas mais cancelamentos
-- 
-- SERVIÇOS:
-- - Design Simples: Alto volume, alto cancelamento (~28%)
-- - Hidrolipo NA: Volume médio, baixo cancelamento (~10%)
-- - Massagens: Alto volume, cancelamento médio (~18%)
-- ============================================================================

INSERT INTO agendamento (fk_servico, fk_cliente, fk_funcionario, dt_hora, valor_pago, status_agendamento, status, dt_validade, fk_pacote) VALUES

-- ========== SEMANA 1: 03-08 de Novembro (Segunda a Sábado) ==========
-- Segunda 03/11 - DIA FRACO (apenas 4 agendamentos - oportunidade de promoção!)
(8, 22, 1, '2025-11-03 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),  -- Design Simples - Jovem
(8, 23, 1, '2025-11-03 10:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),  -- Design Simples - CANCELOU
(4, 32, 1, '2025-11-03 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL), -- Massagem Relaxante - 40+
(2, 3, 1, '2025-11-03 16:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),  -- Drenagem - VIP

-- Terça 04/11 - DIA FRACO (apenas 5 agendamentos)
(8, 24, 1, '2025-11-04 09:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),  -- Design Simples - CANCELOU
(9, 7, 1, '2025-11-04 10:00:00', 35.00, 'Concluido', 'Concluido', NULL, NULL),   -- Depilação
(1, 2, 1, '2025-11-04 11:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),   -- Massagem Modeladora - VIP
(6, 13, 1, '2025-11-04 14:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL), -- Limpeza de Pele
(8, 25, 1, '2025-11-04 17:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),  -- Design Simples - CANCELOU

-- Quarta 05/11 - DIA BOM (8 agendamentos)
(3, 33, 1, '2025-11-05 08:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL), -- Hidrolipo NA - 40+ (não cancela)
(8, 8, 1, '2025-11-05 09:30:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),   -- Design Simples
(7, 4, 1, '2025-11-05 10:30:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),   -- Design Henna - VIP
(1, 14, 1, '2025-11-05 12:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),  -- Massagem Modeladora
(2, 5, 1, '2025-11-05 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),  -- Drenagem - VIP
(5, 15, 1, '2025-11-05 15:30:00', 180.00, 'Concluido', 'Concluido', NULL, NULL), -- Enzimas
(8, 26, 1, '2025-11-05 17:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),  -- Design Simples - CANCELOU
(4, 34, 1, '2025-11-05 18:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL), -- Massagem Relaxante - 40+

-- Quinta 06/11 - MELHOR DIA! (10 agendamentos, apenas 1 cancelamento)
(3, 35, 1, '2025-11-06 08:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL), -- Hidrolipo NA
(1, 2, 1, '2025-11-06 09:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),   -- Massagem - VIP
(8, 9, 1, '2025-11-06 10:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),   -- Design Simples
(7, 16, 1, '2025-11-06 11:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),  -- Design Henna
(6, 3, 1, '2025-11-06 12:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),  -- Limpeza - VIP
(2, 17, 1, '2025-11-06 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL), -- Drenagem
(10, 36, 1, '2025-11-06 15:30:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),-- Detox - 40+
(11, 18, 1, '2025-11-06 17:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL), -- Pump Up
(8, 27, 1, '2025-11-06 18:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),  -- Design - CANCELOU
(4, 37, 1, '2025-11-06 19:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL), -- Massagem - 40+

-- Sexta 07/11 - DIA BOM (8 agendamentos)
(8, 10, 1, '2025-11-07 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),  -- Design Simples
(9, 11, 1, '2025-11-07 10:00:00', 35.00, 'Concluido', 'Concluido', NULL, NULL),  -- Depilação
(1, 4, 1, '2025-11-07 11:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),   -- Massagem - VIP
(3, 38, 1, '2025-11-07 12:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL), -- Hidrolipo - 40+
(2, 19, 1, '2025-11-07 14:30:00', 100.00, 'Concluido', 'Concluido', NULL, NULL), -- Drenagem
(8, 28, 1, '2025-11-07 16:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),  -- Design - CANCELOU
(7, 20, 1, '2025-11-07 17:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),  -- Design Henna
(4, 5, 1, '2025-11-07 18:30:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),  -- Massagem - VIP

-- Sábado 08/11 - ALTA DEMANDA MAS MAIS CANCELAMENTOS (12 agendamentos, 4 cancelamentos)
(8, 21, 1, '2025-11-08 08:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),  -- Design - CANCELOU (sábado)
(8, 12, 1, '2025-11-08 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),  -- Design Simples
(7, 6, 1, '2025-11-08 10:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),   -- Design Henna - VIP
(9, 29, 1, '2025-11-08 11:00:00', 35.00, 'Cancelado', 'Cancelado', NULL, NULL),  -- Depilação - CANCELOU
(1, 41, 1, '2025-11-08 12:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),  -- Massagem
(6, 39, 1, '2025-11-08 13:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL), -- Limpeza - 40+
(2, 2, 1, '2025-11-08 14:30:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),  -- Drenagem - VIP
(3, 40, 1, '2025-11-08 15:30:00', 180.00, 'Concluido', 'Concluido', NULL, NULL), -- Hidrolipo - 40+
(8, 30, 1, '2025-11-08 17:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),  -- Design - CANCELOU
(11, 42, 1, '2025-11-08 18:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL), -- Pump Up
(4, 43, 1, '2025-11-08 19:00:00', 100.00, 'Cancelado', 'Cancelado', NULL, NULL), -- Massagem - CANCELOU
(5, 3, 1, '2025-11-08 20:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),  -- Enzimas - VIP

-- ========== SEMANA 2: 10-15 de Novembro ==========
-- Segunda 10/11 - DIA FRACO
(8, 44, 1, '2025-11-10 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(4, 31, 1, '2025-11-10 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 22, 1, '2025-11-10 16:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),

-- Terça 11/11 - DIA FRACO
(2, 4, 1, '2025-11-11 10:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 23, 1, '2025-11-11 11:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),
(9, 45, 1, '2025-11-11 14:00:00', 35.00, 'Concluido', 'Concluido', NULL, NULL),
(1, 32, 1, '2025-11-11 16:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),

-- Quarta 12/11 - DIA BOM
(3, 33, 1, '2025-11-12 08:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 7, 1, '2025-11-12 09:30:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(7, 5, 1, '2025-11-12 10:30:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),
(6, 46, 1, '2025-11-12 12:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 34, 1, '2025-11-12 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(10, 8, 1, '2025-11-12 15:30:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 24, 1, '2025-11-12 17:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),

-- Quinta 13/11 - MELHOR DIA
(3, 35, 1, '2025-11-13 08:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(1, 3, 1, '2025-11-13 09:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 9, 1, '2025-11-13 10:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(7, 47, 1, '2025-11-13 11:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),
(5, 2, 1, '2025-11-13 12:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 36, 1, '2025-11-13 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(11, 10, 1, '2025-11-13 15:30:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(4, 37, 1, '2025-11-13 17:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 25, 1, '2025-11-13 18:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),

-- Sexta 14/11 - DIA BOM
(8, 11, 1, '2025-11-14 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(9, 48, 1, '2025-11-14 10:00:00', 35.00, 'Concluido', 'Concluido', NULL, NULL),
(1, 5, 1, '2025-11-14 11:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(3, 38, 1, '2025-11-14 12:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(6, 12, 1, '2025-11-14 14:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 26, 1, '2025-11-14 16:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),
(2, 4, 1, '2025-11-14 17:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),

-- Sábado 15/11 - FERIADO (Proclamação da República) - Fechado, sem agendamentos

-- ========== SEMANA 3: 17-22 de Novembro (queda leve - meio do mês) ==========
-- Segunda 17/11 - DIA MUITO FRACO (apenas 3 agendamentos!)
(8, 27, 1, '2025-11-17 10:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),
(4, 39, 1, '2025-11-17 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 6, 1, '2025-11-17 16:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),

-- Terça 18/11 - DIA FRACO
(8, 13, 1, '2025-11-18 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(9, 28, 1, '2025-11-18 10:00:00', 35.00, 'Cancelado', 'Cancelado', NULL, NULL),
(1, 40, 1, '2025-11-18 14:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(7, 49, 1, '2025-11-18 16:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),

-- Quarta 19/11 - DIA MODERADO
(3, 31, 1, '2025-11-19 08:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 14, 1, '2025-11-19 09:30:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(6, 2, 1, '2025-11-19 11:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 32, 1, '2025-11-19 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 29, 1, '2025-11-19 16:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),
(4, 50, 1, '2025-11-19 18:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),

-- Quinta 20/11 - FERIADO (Consciência Negra) - Funcionando, alta demanda!
(3, 33, 1, '2025-11-20 08:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 15, 1, '2025-11-20 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(7, 3, 1, '2025-11-20 10:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),
(1, 34, 1, '2025-11-20 11:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(5, 4, 1, '2025-11-20 12:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 35, 1, '2025-11-20 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(10, 16, 1, '2025-11-20 15:30:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(11, 36, 1, '2025-11-20 17:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 30, 1, '2025-11-20 18:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),
(4, 5, 1, '2025-11-20 19:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),

-- Sexta 21/11 - DIA BOM
(8, 17, 1, '2025-11-21 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(9, 18, 1, '2025-11-21 10:00:00', 35.00, 'Concluido', 'Concluido', NULL, NULL),
(3, 37, 1, '2025-11-21 11:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(6, 6, 1, '2025-11-21 13:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 38, 1, '2025-11-21 15:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 21, 1, '2025-11-21 16:30:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),
(1, 19, 1, '2025-11-21 17:30:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),

-- Sábado 22/11 - ALTA DEMANDA (pré Black Friday)
(8, 20, 1, '2025-11-22 08:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(7, 2, 1, '2025-11-22 09:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),
(9, 22, 1, '2025-11-22 10:00:00', 35.00, 'Cancelado', 'Cancelado', NULL, NULL),
(1, 39, 1, '2025-11-22 11:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(3, 40, 1, '2025-11-22 12:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(6, 3, 1, '2025-11-22 14:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 41, 1, '2025-11-22 15:30:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 23, 1, '2025-11-22 16:30:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),
(11, 42, 1, '2025-11-22 17:30:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(5, 4, 1, '2025-11-22 19:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),

-- ========== SEMANA 4: 24-29 de Novembro (Black Friday Week!) ==========
-- Segunda 24/11 - DIA MODERADO (início Black Friday Week)
(8, 43, 1, '2025-11-24 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(4, 31, 1, '2025-11-24 11:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 5, 1, '2025-11-24 14:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 24, 1, '2025-11-24 15:30:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),
(9, 44, 1, '2025-11-24 17:00:00', 35.00, 'Concluido', 'Concluido', NULL, NULL),

-- Terça 25/11 - DIA MODERADO
(1, 32, 1, '2025-11-25 09:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 45, 1, '2025-11-25 10:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(7, 6, 1, '2025-11-25 11:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),
(3, 33, 1, '2025-11-25 12:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 46, 1, '2025-11-25 14:30:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 25, 1, '2025-11-25 16:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),

-- Quarta 26/11 - DIA MUITO BOM
(3, 34, 1, '2025-11-26 08:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 7, 1, '2025-11-26 09:30:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(6, 2, 1, '2025-11-26 10:30:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(1, 35, 1, '2025-11-26 12:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(5, 47, 1, '2025-11-26 14:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 36, 1, '2025-11-26 15:30:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(10, 8, 1, '2025-11-26 17:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 26, 1, '2025-11-26 18:30:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),

-- Quinta 27/11 - DIA EXCELENTE (véspera de Black Friday)
(3, 37, 1, '2025-11-27 08:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 9, 1, '2025-11-27 09:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(7, 3, 1, '2025-11-27 10:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),
(1, 38, 1, '2025-11-27 11:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(6, 48, 1, '2025-11-27 12:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(5, 4, 1, '2025-11-27 14:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 39, 1, '2025-11-27 15:30:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(11, 10, 1, '2025-11-27 17:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(4, 40, 1, '2025-11-27 18:00:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 27, 1, '2025-11-27 19:00:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),

-- Sexta 28/11 - BLACK FRIDAY! (dia mais movimentado do mês)
(3, 31, 1, '2025-11-28 08:00:00', 162.00, 'Concluido', 'Concluido', NULL, NULL), -- 10% desc
(8, 11, 1, '2025-11-28 09:00:00', 27.00, 'Concluido', 'Concluido', NULL, NULL),  -- 10% desc
(7, 5, 1, '2025-11-28 09:30:00', 40.50, 'Concluido', 'Concluido', NULL, NULL),   -- 10% desc
(9, 49, 1, '2025-11-28 10:00:00', 31.50, 'Concluido', 'Concluido', NULL, NULL),  -- 10% desc
(1, 32, 1, '2025-11-28 10:30:00', 81.00, 'Concluido', 'Concluido', NULL, NULL),  -- 10% desc
(6, 2, 1, '2025-11-28 11:30:00', 135.00, 'Concluido', 'Concluido', NULL, NULL),  -- 10% desc
(5, 33, 1, '2025-11-28 13:30:00', 162.00, 'Concluido', 'Concluido', NULL, NULL), -- 10% desc
(2, 6, 1, '2025-11-28 14:30:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),   -- 10% desc
(10, 34, 1, '2025-11-28 15:30:00', 135.00, 'Concluido', 'Concluido', NULL, NULL),-- 10% desc
(8, 28, 1, '2025-11-28 16:30:00', 27.00, 'Cancelado', 'Cancelado', NULL, NULL),
(11, 50, 1, '2025-11-28 17:00:00', 81.00, 'Concluido', 'Concluido', NULL, NULL), -- 10% desc
(4, 35, 1, '2025-11-28 18:00:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),  -- 10% desc
(8, 12, 1, '2025-11-28 19:00:00', 27.00, 'Concluido', 'Concluido', NULL, NULL),  -- 10% desc
(3, 36, 1, '2025-11-28 19:30:00', 162.00, 'Concluido', 'Concluido', NULL, NULL), -- 10% desc

-- Sábado 29/11 - PÓS BLACK FRIDAY (ainda alta demanda)
(8, 13, 1, '2025-11-29 08:00:00', 30.00, 'Concluido', 'Concluido', NULL, NULL),
(7, 3, 1, '2025-11-29 09:00:00', 45.00, 'Concluido', 'Concluido', NULL, NULL),
(9, 29, 1, '2025-11-29 10:00:00', 35.00, 'Cancelado', 'Cancelado', NULL, NULL),
(1, 37, 1, '2025-11-29 10:30:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(6, 4, 1, '2025-11-29 12:00:00', 150.00, 'Concluido', 'Concluido', NULL, NULL),
(3, 38, 1, '2025-11-29 14:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL),
(2, 14, 1, '2025-11-29 15:30:00', 100.00, 'Concluido', 'Concluido', NULL, NULL),
(8, 30, 1, '2025-11-29 16:30:00', 30.00, 'Cancelado', 'Cancelado', NULL, NULL),
(11, 39, 1, '2025-11-29 17:30:00', 90.00, 'Concluido', 'Concluido', NULL, NULL),
(5, 5, 1, '2025-11-29 19:00:00', 180.00, 'Concluido', 'Concluido', NULL, NULL);

-- ============================================================================
-- AGENDAMENTOS FUTUROS (DEZEMBRO 2025) - Para mostrar pipeline
-- Status: Agendado (não concluídos ainda)
-- ============================================================================

INSERT INTO agendamento (fk_servico, fk_cliente, fk_funcionario, dt_hora, valor_pago, status_agendamento, status, dt_validade, fk_pacote) VALUES
-- Dezembro - primeiros dias
(3, 2, 1, '2025-12-01 08:00:00', 180.00, 'Agendado', 'Agendado', NULL, NULL),
(8, 15, 1, '2025-12-01 09:00:00', 30.00, 'Agendado', 'Agendado', NULL, NULL),
(7, 6, 1, '2025-12-01 10:00:00', 45.00, 'Agendado', 'Agendado', NULL, NULL),
(1, 40, 1, '2025-12-01 11:00:00', 90.00, 'Agendado', 'Agendado', NULL, NULL),
(6, 3, 1, '2025-12-02 09:00:00', 150.00, 'Agendado', 'Agendado', NULL, NULL),
(2, 41, 1, '2025-12-02 14:00:00', 100.00, 'Agendado', 'Agendado', NULL, NULL),
(8, 16, 1, '2025-12-03 10:00:00', 30.00, 'Agendado', 'Agendado', NULL, NULL),
(3, 42, 1, '2025-12-03 14:00:00', 180.00, 'Agendado', 'Agendado', NULL, NULL),
(5, 4, 1, '2025-12-04 08:00:00', 180.00, 'Agendado', 'Agendado', NULL, NULL),
(11, 43, 1, '2025-12-04 15:00:00', 90.00, 'Agendado', 'Agendado', NULL, NULL);

-- ============================================================================
-- QUERIES DE VALIDAÇÃO
-- ============================================================================

-- Verificar distribuição por dia da semana
SELECT 
    DAYNAME(dt_hora) as dia_semana,
    COUNT(*) as total_agendamentos,
    SUM(CASE WHEN status = 'Cancelado' THEN 1 ELSE 0 END) as cancelamentos,
    ROUND(SUM(CASE WHEN status = 'Cancelado' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as taxa_cancelamento
FROM agendamento
WHERE dt_hora BETWEEN '2025-11-01' AND '2025-11-30'
GROUP BY DAYNAME(dt_hora), DAYOFWEEK(dt_hora)
ORDER BY DAYOFWEEK(dt_hora);

-- Verificar distribuição por serviço
SELECT 
    s.nome as servico,
    COUNT(*) as total,
    SUM(CASE WHEN a.status = 'Cancelado' THEN 1 ELSE 0 END) as cancelados,
    ROUND(SUM(CASE WHEN a.status = 'Cancelado' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as taxa_cancelamento,
    SUM(CASE WHEN a.status = 'Concluido' THEN a.valor_pago ELSE 0 END) as faturamento
FROM agendamento a
JOIN servico s ON a.fk_servico = s.id_servico
WHERE a.dt_hora BETWEEN '2025-11-01' AND '2025-11-30'
GROUP BY s.nome
ORDER BY total DESC;

-- KPIs Gerais do Mês
SELECT 
    COUNT(*) as total_agendamentos,
    SUM(CASE WHEN status = 'Concluido' THEN 1 ELSE 0 END) as concluidos,
    SUM(CASE WHEN status = 'Cancelado' THEN 1 ELSE 0 END) as cancelados,
    ROUND(SUM(CASE WHEN status = 'Cancelado' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as taxa_cancelamento,
    SUM(CASE WHEN status = 'Concluido' THEN valor_pago ELSE 0 END) as faturamento_total
FROM agendamento
WHERE dt_hora BETWEEN '2025-11-01' AND '2025-11-30';

-- Top 5 Clientes VIP
SELECT 
    u.nome as cliente,
    COUNT(*) as total_consultas,
    SUM(CASE WHEN a.status = 'Concluido' THEN a.valor_pago ELSE 0 END) as valor_gasto
FROM agendamento a
JOIN usuario u ON a.fk_cliente = u.id_usuario
WHERE a.dt_hora BETWEEN '2025-11-01' AND '2025-11-30'
    AND a.status = 'Concluido'
GROUP BY u.nome
ORDER BY valor_gasto DESC
LIMIT 5;

SELECT '✅ Dados mockados inseridos com sucesso! Execute as queries acima para validar.' as resultado;