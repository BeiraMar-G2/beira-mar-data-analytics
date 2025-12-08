#!/bin/bash
# ============================================================
# DEPLOY AUTOMÁTICO - BEIRA-MAR ANALYTICS DASHBOARD
# Coloque este arquivo na RAIZ do seu repositório GitHub
# Nome do arquivo: deploy_dash.sh
# 
# USO:
#   git clone https://github.com/BeiraMar-G2/beira-mar-data-analytics.git
#   cd beira-mar-data-analytics/
#   bash deploy_dash.sh
#   ./start-dashboard.sh
# ============================================================

set -e  # Parar em caso de erro

echo "🚀 Iniciando Deploy da Dashboard Beira-Mar Analytics..."
echo "========================================================"
echo ""

# Verificar se está no diretório correto
if [ ! -f "cloud/analise/dashboard_beira_mar.py" ]; then
    echo "❌ Erro: Arquivo dashboard_beira_mar.py não encontrado!"
    echo "   Certifique-se de estar na raiz do repositório."
    exit 1
fi

echo "✅ Diretório correto identificado"
echo ""

# 1. ATUALIZAR SISTEMA
echo "📦 [1/7] Atualizando sistema..."
sudo apt update > /dev/null 2>&1
sudo apt upgrade -y > /dev/null 2>&1
echo "✅ Sistema atualizado"
echo ""

# 2. INSTALAR DEPENDÊNCIAS DO SISTEMA
echo "📚 [2/7] Instalando Python, Git e dependências do sistema..."
sudo apt install -y \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    git \
    build-essential \
    tmux > /dev/null 2>&1
echo "✅ Dependências do sistema instaladas"
echo ""

# 3. REMOVER AMBIENTE VIRTUAL ANTIGO (se existir)
if [ -d "venv" ]; then
    echo "🗑️  [3/7] Removendo ambiente virtual antigo..."
    rm -rf venv
    echo "✅ Ambiente virtual antigo removido"
else
    echo "✅ [3/7] Nenhum ambiente virtual antigo encontrado"
fi
echo ""

# 4. CRIAR NOVO AMBIENTE VIRTUAL
echo "🐍 [4/7] Criando novo ambiente virtual..."
python3 -m venv venv
echo "✅ Ambiente virtual criado"
echo ""

# 5. ATIVAR AMBIENTE VIRTUAL E INSTALAR DEPENDÊNCIAS
echo "📦 [5/7] Instalando bibliotecas Python no ambiente virtual..."
# Usar caminho absoluto do pip do venv para garantir instalação correta
./venv/bin/pip install --upgrade pip > /dev/null 2>&1

# Verificar se existe requirements.txt
if [ -f "requirements.txt" ]; then
    echo "   📄 Instalando via requirements.txt..."
    ./venv/bin/pip install -r requirements.txt > /dev/null 2>&1
else
    echo "   📦 Instalando bibliotecas manualmente..."
    ./venv/bin/pip install \
        streamlit \
        pandas \
        numpy \
        plotly \
        emoji \
        wordcloud \
        matplotlib \
        openpyxl > /dev/null 2>&1
fi
echo "✅ Bibliotecas Python instaladas"
echo ""

# 6. VERIFICAR INSTALAÇÃO DO STREAMLIT
echo "🔍 [6/7] Verificando instalação do Streamlit..."
if ./venv/bin/streamlit --version > /dev/null 2>&1; then
    STREAMLIT_VERSION=$(./venv/bin/streamlit --version)
    echo "✅ Streamlit instalado: $STREAMLIT_VERSION"
else
    echo "❌ Erro: Streamlit não foi instalado corretamente"
    exit 1
fi
echo ""

# 7. CRIAR SCRIPTS AUXILIARES
echo "📝 [7/7] Criando scripts auxiliares..."

# ----------------------------------------------------------
# SCRIPT: start-dashboard.sh
# ----------------------------------------------------------
cat > start-dashboard.sh << 'STARTSCRIPT'
#!/bin/bash
# Script para iniciar a dashboard em modo background com tmux

# Verificar se já existe uma sessão
if tmux has-session -t dashboard 2>/dev/null; then
    echo "⚠️  Sessão 'dashboard' já existe!"
    echo "   Para ver: tmux attach -t dashboard"
    echo "   Para matar: ./stop-dashboard.sh"
    exit 1
fi

# Obter diretório atual
CURRENT_DIR=$(pwd)

# Criar nova sessão tmux
echo "🚀 Iniciando dashboard em background..."
tmux new-session -d -s dashboard

# Executar comandos na sessão
tmux send-keys -t dashboard "cd $CURRENT_DIR" C-m
tmux send-keys -t dashboard "source venv/bin/activate" C-m
tmux send-keys -t dashboard "streamlit run cloud/analise/dashboard_beira_mar.py --server.port=8501 --server.address=0.0.0.0" C-m

# Aguardar 3 segundos para o streamlit iniciar
sleep 3

# Verificar se realmente iniciou
if pgrep -f "streamlit" > /dev/null; then
    echo "✅ Dashboard iniciada com sucesso!"
    echo "🌐 Acesse: http://$(curl -s ifconfig.me 2>/dev/null || echo 'SEU_IP'):8501"
    echo ""
    echo "📺 Para ver os logs: tmux attach -t dashboard"
    echo "   (Para sair sem fechar: Ctrl+B depois D)"
    echo ""
    echo "🛑 Para parar: ./stop-dashboard.sh"
else
    echo "❌ Erro ao iniciar dashboard"
    echo "   Verifique os logs: tmux attach -t dashboard"
    exit 1
fi
STARTSCRIPT

chmod +x start-dashboard.sh

# ----------------------------------------------------------
# SCRIPT: stop-dashboard.sh
# ----------------------------------------------------------
cat > stop-dashboard.sh << 'STOPSCRIPT'
#!/bin/bash
# Script para parar a dashboard

if tmux has-session -t dashboard 2>/dev/null; then
    echo "🛑 Parando dashboard..."
    tmux kill-session -t dashboard
    sleep 1
    
    # Verificar se realmente parou
    if pgrep -f "streamlit" > /dev/null; then
        echo "⚠️  Processo streamlit ainda ativo, forçando encerramento..."
        pkill -f streamlit
    fi
    
    echo "✅ Dashboard parada!"
else
    echo "⚠️  Nenhuma sessão 'dashboard' encontrada"
    
    # Verificar se há processo streamlit rodando
    if pgrep -f "streamlit" > /dev/null; then
        echo "🔍 Encontrado processo streamlit rodando, encerrando..."
        pkill -f streamlit
        echo "✅ Processo streamlit encerrado!"
    fi
fi
STOPSCRIPT

chmod +x stop-dashboard.sh

# ----------------------------------------------------------
# SCRIPT: status-dashboard.sh
# ----------------------------------------------------------
cat > status-dashboard.sh << 'STATUSSCRIPT'
#!/bin/bash
# Script para verificar status da dashboard

echo "📊 Status da Dashboard Beira-Mar"
echo "================================"
echo ""

# Verificar sessão tmux
TMUX_RUNNING=false
if tmux has-session -t dashboard 2>/dev/null; then
    echo "✅ Sessão tmux 'dashboard' está ATIVA"
    TMUX_RUNNING=true
else
    echo "❌ Sessão tmux 'dashboard' NÃO encontrada"
fi

echo ""

# Verificar se o processo streamlit está rodando
STREAMLIT_RUNNING=false
if pgrep -f "streamlit" > /dev/null; then
    echo "✅ Processo Streamlit está RODANDO"
    STREAMLIT_RUNNING=true
    echo ""
    echo "📊 Uso de recursos:"
    ps aux | grep streamlit | grep -v grep | awk '{print "   CPU: "$3"% | RAM: "$4"% | PID: "$2}'
else
    echo "❌ Processo Streamlit NÃO está ativo"
fi

echo ""
echo "🌐 Informações de acesso:"
PUBLIC_IP=$(curl -s ifconfig.me 2>/dev/null || echo "Não foi possível obter IP")
echo "   IP Público: $PUBLIC_IP"
echo "   URL: http://$PUBLIC_IP:8501"

echo ""

# Status geral
if [ "$TMUX_RUNNING" = true ] && [ "$STREAMLIT_RUNNING" = true ]; then
    echo "🟢 STATUS GERAL: OPERACIONAL"
    echo ""
    echo "📺 Comandos úteis:"
    echo "   Ver logs:  tmux attach -t dashboard"
    echo "   Parar:     ./stop-dashboard.sh"
elif [ "$STREAMLIT_RUNNING" = true ]; then
    echo "🟡 STATUS GERAL: RODANDO (sem tmux)"
    echo ""
    echo "🛑 Para parar: ./stop-dashboard.sh"
else
    echo "🔴 STATUS GERAL: PARADO"
    echo ""
    echo "🚀 Para iniciar: ./start-dashboard.sh"
fi

echo ""
echo "================================"
STATUSSCRIPT

chmod +x status-dashboard.sh

echo "✅ Scripts criados:"
echo "   - start-dashboard.sh  (iniciar em background)"
echo "   - stop-dashboard.sh   (parar dashboard)"
echo "   - status-dashboard.sh (verificar status)"
echo ""

# 8. OBTER IP PÚBLICO
PUBLIC_IP=$(curl -s ifconfig.me 2>/dev/null || echo "Não foi possível obter IP")

echo ""
echo "========================================================"
echo "✅ INSTALAÇÃO CONCLUÍDA COM SUCESSO!"
echo "========================================================"
echo ""
echo "🌐 IP Público: $PUBLIC_IP"
echo ""
echo "🚀 Para iniciar a dashboard agora, execute:"
echo ""
echo "   ./start-dashboard.sh"
echo ""
echo "🌐 Depois acesse: http://$PUBLIC_IP:8501"
echo ""
echo "========================================================"
echo "⚠️  IMPORTANTE: Certifique-se de liberar a porta 8501"
echo "   no Security Group da EC2!"
echo ""
echo "   AWS Console → EC2 → Security Groups"
echo "   → Add Inbound Rule:"
echo "   Type: Custom TCP | Port: 8501 | Source: 0.0.0.0/0"
echo "========================================================"
echo ""
echo "📝 Comandos úteis:"
echo "   ./start-dashboard.sh  - Iniciar dashboard"
echo "   ./stop-dashboard.sh   - Parar dashboard"
echo "   ./status-dashboard.sh - Ver status"
echo ""
echo "========================================================"