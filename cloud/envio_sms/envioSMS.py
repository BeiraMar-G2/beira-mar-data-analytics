from twilio.rest import Client
import dotenv
import os
import mysql.connector
from datetime import datetime, timedelta
import re

# Carregar variáveis de ambiente com override
dotenv.load_dotenv(override=True)
twToken = os.getenv("TWILIO_TOKEN")
twNum = os.getenv("TWILIO_NUMERO")
twSid = os.getenv("TWILIO_SID")
senhaBD = os.getenv("SENHA_DB")
numCliente = os.getenv("NUM_CLIENTE")

def obter_consultas():
    """Busca consultas do banco de dados"""
    try:
        db_connection = mysql.connector.connect(
            host='localhost', 
            user='root', 
            password=senhaBD, 
            database='beiraMar'
        )
        cursor = db_connection.cursor()

        cursor.execute('''
            SELECT u.nome, u.telefone, s.nome, a.dt_hora 
            FROM agendamento as a 
            JOIN usuario as u ON a.fk_cliente = u.id_usuario 
            JOIN servico as s ON a.fk_servico = s.id_servico
            WHERE DATE(a.dt_hora) IN (CURDATE(), DATE_ADD(CURDATE(), INTERVAL 1 DAY))
        ''')

        resultados = cursor.fetchall()
        return resultados
    
    except mysql.connector.Error as err:
        print(f"❌ Erro no banco de dados: {err}")
        return []
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'db_connection' in locals():
            db_connection.close()

def filtrar_consultas(resultados):
    """Filtra consultas por data"""
    listaConsultasAmanha = []
    listaConsultasHoje = []
    
    amanha = (datetime.now() + timedelta(days=1)).date()
    hoje = datetime.now().date()

    for linha in resultados:
        if linha[3].date() == amanha:
            listaConsultasAmanha.append(linha)
        elif linha[3].date() == hoje:
            listaConsultasHoje.append(linha)
    
    return listaConsultasHoje, listaConsultasAmanha

def montar_mensagem(nome_cliente, horario_consulta, dia):
    """Monta a mensagem de lembrete"""
    hora_formatada = horario_consulta.strftime("%H:%M")
    if dia == "hoje":
        return f"{nome_cliente}, sua consulta na clínica Beira-Mar é hoje às {hora_formatada}. Dúvidas? Ligue: +55 11 987654321."
    elif dia == "amanha":
        return f"{nome_cliente}, lembrando que sua consulta na clínica Beira-Mar é amanhã às {hora_formatada}. Dúvidas? +55 11 987654321."

def enviar_sms_consulta(client, numero_destino, nome_cliente, horario_consulta, dia):
    """Envia SMS de lembrete"""
    try:
        mensagem = montar_mensagem(nome_cliente, horario_consulta, dia)
        
        message = client.messages.create(
            body=mensagem,
            from_=twNum,
            to=numero_destino
        )
        print(f"✅ SMS enviado para {nome_cliente}")
        print(f"   SID: {message.sid}")
        print(f"   Status: {message.status}\n")
        return True
    
    except Exception as e:
        print(f"❌ Erro ao enviar para {nome_cliente}:")
        print(f"   {str(e)}\n")
        return False

def main():
    """Função principal"""
    print("\n" + "="*60)
    print("🏥 SISTEMA DE LEMBRETES - CLÍNICA BEIRA-MAR")
    print("="*60 + "\n")
    
    # Verificar credenciais
    if not all([twSid, twToken, twNum, senhaBD, numCliente]):
        print("❌ Erro: Variáveis de ambiente faltando no .env\n")
        return
    
    print("✅ Credenciais carregadas:")
    print(f"   Account SID: {twSid[:15]}...")
    print(f"   Número Twilio: {twNum}")
    print(f"   Destino (teste): {numCliente}\n")
    
    # Inicializar cliente Twilio
    try:
        client = Client(twSid, twToken)
        account = client.api.accounts(twSid).fetch()
        print(f"✅ Conectado à conta: {account.friendly_name}")
        print(f"   Tipo: {account.type}\n")
        
        if account.type == 'Trial':
            print("⚠️  ATENÇÃO: Você está em conta TRIAL")
            print("   Certifique-se que o número destino está verificado em:")
            print("   https://console.twilio.com/us1/develop/phone-numbers/manage/verified\n")
        
    except Exception as e:
        print(f"❌ Erro ao conectar no Twilio: {e}\n")
        return
    
    # Obter consultas
    print("🔍 Buscando consultas no banco de dados...")
    resultados = obter_consultas()
    
    if not resultados:
        print("ℹ️  Nenhuma consulta encontrada para hoje ou amanhã\n")
        return
    
    # Filtrar por data
    listaConsultasHoje, listaConsultasAmanha = filtrar_consultas(resultados)
    
    print(f"✅ Encontradas {len(listaConsultasHoje)} consulta(s) para HOJE")
    print(f"✅ Encontradas {len(listaConsultasAmanha)} consulta(s) para AMANHÃ\n")
    
    # Enviar mensagens
    enviados = 0
    erros = 0
    
    print("="*60)
    print("📨 MODO TESTE - Enviando todas as mensagens para:", numCliente)
    print("="*60 + "\n")
    
    if listaConsultasAmanha:
        print("📅 LEMBRETES DE AMANHÃ:\n")
        for consulta in listaConsultasAmanha:
            nome = consulta[0]
            horario = consulta[3]
            
            if enviar_sms_consulta(client, numCliente, nome, horario, "amanha"):
                enviados += 1
            else:
                erros += 1

    if listaConsultasHoje:
        print("📅 LEMBRETES DE HOJE:\n")
        for consulta in listaConsultasHoje:
            nome = consulta[0]
            horario = consulta[3]
            
            if enviar_sms_consulta(client, numCliente, nome, horario, "hoje"):
                enviados += 1
            else:
                erros += 1
    
    print("="*60)
    print("📊 RESUMO FINAL")
    print("="*60)
    print(f"✅ Mensagens enviadas: {enviados}")
    print(f"❌ Erros: {erros}")
    print(f"📱 Total de consultas: {enviados + erros}")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()