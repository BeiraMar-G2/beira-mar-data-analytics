#!/bin/bash
# ============================================================
# DEPLOY AUTOMÁTICO - BEIRA-MAR ANALYTICS DASHBOARD
# Versão com Dependências Integradas
# ============================================================

set -e  # Para o script se houver erro
export DEBIAN_FRONTEND=noninteractive

echo "🚀 Iniciando Deploy da Dashboard Beira-Mar Analytics..."
echo "========================================================"

# Verificar se estamos na pasta certa
FILE_PATH="cloud/analise/dashboard_beira_mar.py"
if [ ! -f "$FILE_PATH" ]; then
    echo "❌ Erro: Arquivo '$FILE_PATH' não encontrado!"
    echo "   Você deve rodar este script dentro da pasta 'beira-mar-data-analytics'"
    exit 1
fi
echo "✅ Diretório correto."
echo ""

# 1. ATUALIZAR SISTEMA
echo "📦 [1/6] Atualizando sistema..."
sudo apt-get update -q
sudo -E apt-get upgrade -y -q
echo "✅ Sistema atualizado"
echo ""

# 2. INSTALAR DEPENDÊNCIAS DO LINUX
echo "📚 [2/6] Instalando dependências do Linux..."
# Adicionei python3-setuptools aqui para garantir
sudo apt-get install -y python3-pip python3-venv python3-dev git build-essential tmux python3-setuptools
echo "✅ Dependências instaladas"
echo ""

# 3. LIMPEZA E CRIAÇÃO DO VENV
if [ -d "venv" ]; then
    echo "🗑️  [3/6] Recriando ambiente virtual..."
    rm -rf venv
else
    echo "🐍 [3/6] Criando ambiente virtual..."
fi
python3 -m venv venv
echo "✅ Ambiente criado"
echo ""

# 4. INSTALAR BIBLIOTECAS PYTHON (AQUI ESTAVA O ERRO)
echo "📦 [4/6] Instalando bibliotecas Python..."

# PASSO CRÍTICO: Atualizar ferramentas de build para evitar erro do NumPy
echo "   🔧 Atualizando pip, setuptools e wheel..."
./venv/bin/pip install --upgrade pip setuptools wheel -q

echo "   ⬇️  Baixando e instalando as libs (Isso pode demorar)..."
# Usamos --no-cache-dir para evitar erro de memória na EC2
./venv/bin/pip install \
    streamlit==1.29.0 \
    pandas==2.1.4 \
    numpy==1.24.3 \
    plotly==5.18.0 \
    emoji==2.9.0 \
    wordcloud==1.9.3 \
    matplotlib==3.8.2 \
    openpyxl==3.1.2 \
    --no-cache-dir

echo "✅ Bibliotecas instaladas com sucesso!"
echo ""

# 5. GERAR SCRIPTS DE CONTROLE
echo "📝 [5/6] Gerando scripts de execução..."

# --- START SCRIPT ---
cat > start-dashboard.sh << 'EOF'
#!/bin/bash
SESSION_NAME="dashboard"
SCRIPT_PATH="cloud/analise/dashboard_beira_mar.py"

if tmux has-session -t $SESSION_NAME 2>/dev/null; then
    echo "⚠️  A dashboard já está rodando (tmux session: $SESSION_NAME)."
    exit 1
fi

echo "🚀 Iniciando Streamlit em background..."
tmux new-session -d -s $SESSION_NAME

# Força o source do venv e execução
tmux send-keys -t $SESSION_NAME "source venv/bin/activate" C-m
tmux send-keys -t $SESSION_NAME "streamlit run $SCRIPT_PATH --server.port=8501 --server.address=0.0.0.0" C-m

sleep 5
if pgrep -f "streamlit" > /dev/null; then
    PUBLIC_IP=$(curl -s ifconfig.me)
    echo "✅ SUCESSO! Dashboard online."
    echo "🌐 http://$PUBLIC_IP:8501"
else
    echo "❌ Falha ao iniciar. Veja o erro com: tmux attach -t dashboard"
fi
EOF

# --- STOP SCRIPT ---
cat > stop-dashboard.sh << 'EOF'
#!/bin/bash
tmux kill-session -t dashboard 2>/dev/null
pkill -f streamlit 2>/dev/null
echo "✅ Dashboard parada."
EOF

chmod +x start-dashboard.sh
chmod +x stop-dashboard.sh

# 6. FINALIZAR
echo ""
echo "========================================================"
echo "✅ DEPLOY FINALIZADO!"
echo "========================================================"
echo "Para iniciar, rode:"
echo "👉 ./start-dashboard.sh"
echo "========================================================"