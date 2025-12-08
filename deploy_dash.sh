#!/bin/bash
# ============================================================
# DEPLOY AUTOMÁTICO - BEIRA-MAR ANALYTICS DASHBOARD
# Versão com Correção de Caminho Relativo (cd cloud/analise)
# ============================================================

set -e
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

# 2. INSTALAR DEPENDÊNCIAS DO LINUX
echo "📚 [2/6] Instalando dependências do Linux..."
sudo apt-get install -y python3-pip python3-venv python3-dev git build-essential tmux python3-setuptools
echo "✅ Dependências instaladas"

# 3. LIMPEZA E CRIAÇÃO DO VENV
if [ -d "venv" ]; then
    # Não vamos apagar se já existir para ganhar tempo, a menos que queira forçar
    echo "✅ Ambiente virtual já existe."
else
    echo "🐍 [3/6] Criando ambiente virtual..."
    python3 -m venv venv
    echo "✅ Ambiente criado"
fi

# 4. INSTALAR BIBLIOTECAS PYTHON
echo "📦 [4/6] Verificando bibliotecas Python..."
./venv/bin/pip install --upgrade pip setuptools wheel -q
./venv/bin/pip install \
    streamlit==1.32.0 \
    pandas==2.2.1 \
    numpy==1.26.4 \
    plotly==5.20.0 \
    emoji==2.10.1 \
    wordcloud==1.9.3 \
    matplotlib==3.8.3 \
    openpyxl==3.1.2 \
    --no-cache-dir -q
echo "✅ Bibliotecas ok"
echo ""

# 5. GERAR SCRIPTS DE CONTROLE (AQUI ESTÁ A CORREÇÃO)
echo "📝 [5/6] Gerando scripts de execução..."

# --- START SCRIPT CORRIGIDO ---
cat > start-dashboard.sh << 'EOF'
#!/bin/bash
SESSION_NAME="dashboard"

# Caminhos Absolutos
BASE_DIR=$(pwd)
SCRIPT_DIR="cloud/analise"
SCRIPT_FILE="dashboard_beira_mar.py"

if tmux has-session -t $SESSION_NAME 2>/dev/null; then
    echo "⚠️  A dashboard já está rodando."
    exit 1
fi

echo "🚀 Iniciando Streamlit em background..."
tmux new-session -d -s $SESSION_NAME

# 1. Ativar o venv (usando caminho absoluto da raiz)
tmux send-keys -t $SESSION_NAME "source $BASE_DIR/venv/bin/activate" C-m

# 2. Entrar na pasta onde o arquivo .py está
# ISSO CORRIGE O ERRO DO CAMINHO RELATIVO (../../)
tmux send-keys -t $SESSION_NAME "cd $SCRIPT_DIR" C-m

# 3. Rodar o streamlit de dentro da pasta
tmux send-keys -t $SESSION_NAME "streamlit run $SCRIPT_FILE --server.port=8501 --server.address=0.0.0.0" C-m

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
echo "✅ SCRIPT ATUALIZADO!"
echo "========================================================"
echo "Para aplicar a correção, pare o antigo e inicie o novo:"
echo "👉 ./stop-dashboard.sh"
echo "👉 ./start-dashboard.sh"
echo "========================================================"