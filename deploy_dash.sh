#!/bin/bash
# ============================================================
# DEPLOY AUTOMÁTICO - BEIRA-MAR ANALYTICS DASHBOARD
# Coloque este arquivo na RAIZ do seu repositório GitHub
# Execute após clonar o repo na EC2
# ============================================================

echo "🚀 Iniciando Deploy da Dashboard Beira-Mar Analytics..."
echo "========================================================"

# Verificar se está no diretório correto
if [ ! -f "cloud/analise/dashboard_beira_mar.py" ]; then
    echo "❌ Erro: Arquivo dashboard_beira_mar.py não encontrado!"
    echo "   Certifique-se de estar na raiz do repositório."
    exit 1
fi

# 1. ATUALIZAR SISTEMA
echo "📦 Atualizando sistema..."
sudo apt update && sudo apt upgrade -y

# 2. INSTALAR DEPENDÊNCIAS DO SISTEMA
echo "📚 Instalando Python, Git e dependências..."
sudo apt install -y python3 python3-pip python3-venv python3-dev git build-essential tmux

# 3. CRIAR AMBIENTE VIRTUAL (se não existir)
if [ ! -d "venv" ]; then
    echo "🐍 Criando ambiente virtual..."
    python3 -m venv venv
else
    echo "✅ Ambiente virtual já existe"
fi

# 4. ATIVAR AMBIENTE VIRTUAL
echo "✅ Ativando ambiente virtual..."
source venv/bin/activate

# 5. ATUALIZAR PIP
echo "⬆️  Atualizando pip..."
pip install --upgrade pip

# 6. INSTALAR BIBLIOTECAS
echo "📚 Instalando bibliotecas Python..."

# Verificar se existe requirements.txt
if [ -f "requirements.txt" ]; then
    echo "📄 Encontrado requirements.txt, instalando..."
    pip install -r requirements.txt
else
    echo "📦 Instalando bibliotecas manualmente..."
    pip install streamlit pandas numpy plotly emoji wordcloud matplotlib openpyxl
fi

# 7. OBTER IP PÚBLICO
PUBLIC_IP=$(curl -s ifconfig.me)

echo ""
echo "========================================================"
echo "✅ Instalação concluída com sucesso!"
echo "========================================================"
echo ""
echo "🌐 IP Público: $PUBLIC_IP"
echo ""
echo "🚀 Para iniciar a dashboard, execute:"
echo ""
echo "   source venv/bin/activate"
echo "   streamlit run cloud/analise/dashboard_beira_mar.py --server.port=8501 --server.address=0.0.0.0"
echo ""
echo "🌐 Depois acesse: http://$PUBLIC_IP:8501"
echo ""
echo "========================================================"
echo "⚠️  IMPORTANTE: Libere a porta 8501 no Security Group da EC2!"
echo "   AWS Console → EC2 → Security Groups → Add Inbound Rule"
echo "   Type: Custom TCP | Port: 8501 | Source: 0.0.0.0/0"
echo "========================================================"
echo ""
echo "💡 Para manter rodando após desconectar do SSH:"
echo ""
echo "   ./start-dashboard.sh"
echo ""
echo "========================================================"

# 8. CRIAR SCRIPT DE INICIALIZAÇÃO
echo "📝 Criando script de inicialização..."
cat > start-dashboard.sh << 'EOF'
#!/bin/bash
# Script para iniciar a dashboard em modo background com tmux

# Verificar se já existe uma sessão
if tmux has-session -t dashboard 2>/dev/null; then
    echo "⚠️  Sessão 'dashboard' já existe!"
    echo "   Para ver: tmux attach -t dashboard"
    echo "   Para matar: tmux kill-session -t dashboard"
    exit 1
fi

# Criar nova sessão tmux
echo "🚀 Iniciando dashboard em background..."
tmux new-session -d -s dashboard

# Executar comandos na sessão
tmux send-keys -t dashboard "cd $(pwd)" C-m
tmux send-keys -t dashboard "source venv/bin/activate" C-m
tmux send-keys -t dashboard "streamlit run cloud/analise/dashboard_beira_mar.py --server.port=8501 --server.address=0.0.0.0" C-m

echo "✅ Dashboard iniciada com sucesso!"
echo "🌐 Acesse: http://$(curl -s ifconfig.me):8501"
echo ""
echo "📺 Para ver os logs: tmux attach -t dashboard"
echo "   (Para sair sem fechar: Ctrl+B depois D)"
echo ""
echo "🛑 Para parar: tmux kill-session -t dashboard"
EOF

chmod +x start-dashboard.sh

# 9. CRIAR SCRIPT DE PARADA
echo "📝 Criando script de parada..."
cat > stop-dashboard.sh << 'EOF'
#!/bin/bash
# Script para parar a dashboard

if tmux has-session -t dashboard 2>/dev/null; then
    echo "🛑 Parando dashboard..."
    tmux kill-session -t dashboard
    echo "✅ Dashboard parada!"
else
    echo "⚠️  Nenhuma sessão 'dashboard' encontrada"
fi
EOF

chmod +x stop-dashboard.sh

# 10. CRIAR SCRIPT DE STATUS
echo "📝 Criando script de status..."
cat > status-dashboard.sh << 'EOF'
#!/bin/bash
# Script para verificar status da dashboard

echo "📊 Status da Dashboard Beira-Mar"
echo "================================"
echo ""

# Verificar se tmux está instalado
if ! command -v tmux &> /dev/null; then
    echo "❌ tmux não está instalado"
    exit 1
fi

# Verificar sessão tmux
if tmux has-session -t dashboard 2>/dev/null; then
    echo "✅ Dashboard está RODANDO"
    echo ""
    echo "📺 Para ver os logs:"
    echo "   tmux attach -t dashboard"
    echo ""
    echo "🛑 Para parar:"
    echo "   ./stop-dashboard.sh"
else
    echo "❌ Dashboard NÃO está rodando"
    echo ""
    echo "🚀 Para iniciar:"
    echo "   ./start-dashboard.sh"
fi

echo ""
echo "🌐 IP Público: $(curl -s ifconfig.me)"
echo "🌐 URL: http://$(curl -s ifconfig.me):8501"
echo ""

# Verificar se o processo streamlit está rodando
if pgrep -f "streamlit" > /dev/null; then
    echo "🟢 Processo streamlit está ativo"
    echo ""
    echo "📊 Uso de recursos:"
    ps aux | grep streamlit | grep -v grep | awk '{print "   CPU: "$3"% | RAM: "$4"%"}'
else
    echo "🔴 Processo streamlit NÃO está ativo"
fi

echo ""
echo "================================"
EOF

chmod +x status-dashboard.sh

echo ""
echo "✅ Scripts auxiliares criados:"
echo "   - start-dashboard.sh  (iniciar em background)"
echo "   - stop-dashboard.sh   (parar dashboard)"
echo "   - status-dashboard.sh (verificar status)"
echo ""