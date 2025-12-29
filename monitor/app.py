import time
import os
import pandas as pd
from sqlalchemy import create_engine, text
from flask import Flask, request, jsonify

app = Flask(__name__)

# Configuração do Banco
DB_URL = "postgresql://grafana:grafana@postgres:5432/checkout"

def get_engine():
    try:
        engine = create_engine(DB_URL)
        # Teste simples de conexão
        with engine.connect() as conn: pass
        return engine
    except Exception as e:
        print(f"Banco indisponível: {e}")
        return None

def init_db():
    """
    Cria tabelas e aplica migrações de esquema automaticamente (Adiciona coluna 'source').
    """
    engine = get_engine()
    if not engine: return
    
    print("--- Verificando Estrutura do Banco de Dados ---")
    try:
        with engine.connect() as conn:
            conn.execute(text("COMMIT")) # Garante que não há transação presa
            
            # 1. Criação das tabelas base
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS transactions_metrics (
                    time_bucket TIMESTAMP,
                    status VARCHAR(50),
                    count INT,
                    source VARCHAR(20) DEFAULT 'teste'
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS anomalies (
                    id SERIAL PRIMARY KEY,
                    event_time TIMESTAMP,
                    metric VARCHAR(50),
                    value_detected INT,
                    threshold_limit DECIMAL(10,2),
                    message TEXT,
                    source VARCHAR(20) DEFAULT 'teste'
                );
            """))
            
            # 2. MIGRAÇÃO AUTOMÁTICA
            # Garante que a coluna 'source' exista (para quem rodou versões antigas)
            print("Aplicando verificação de colunas...")
            try:
                conn.execute(text("ALTER TABLE transactions_metrics ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'teste'"))
                conn.execute(text("ALTER TABLE anomalies ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'teste'"))
            except Exception as e:
                print(f"Aviso na migração (pode ser ignorado se colunas existem): {e}")
            
            # 3. CORREÇÃO DE DADOS ANTIGOS
            # Se havia dados sem 'source' (NULL), assumimos que são do histórico.
            conn.execute(text("UPDATE transactions_metrics SET source = 'historico' WHERE source IS NULL"))
            conn.execute(text("UPDATE anomalies SET source = 'historico' WHERE source IS NULL"))
            
            conn.commit()
            print("--- Banco de Dados Pronto ---")
            
    except Exception as e:
        print(f"Erro no init_db: {e}")

# --- IMPORTAÇÃO DE HISTÓRICO ---
def import_initial_csvs():
    engine = get_engine()
    if not engine: return
    
    # Verifica se já temos dados de histórico para não duplicar
    with engine.connect() as conn:
        try:
            res = conn.execute(text("SELECT count(*) FROM transactions_metrics WHERE source = 'historico'"))
            if res.scalar() > 0:
                print("--- Dados históricos já carregados. Pulando importação. ---")
                return
        except: pass

    print("--- Iniciando Importação de CSVs ---")
    data_dir = "/data"
    today = pd.Timestamp.now().normalize()

    if not os.path.exists(data_dir):
        print(f"Pasta {data_dir} não encontrada.")
        return

    for file in os.listdir(data_dir):
        if file.endswith(".csv") and "transactions" in file:
            print(f"Lendo arquivo: {file}")
            try:
                df = pd.read_csv(os.path.join(data_dir, file))
                if 'f0_' in df.columns: df = df.rename(columns={'f0_': 'count'})
                
                # Tratamento de Data para "Hoje"
                def parse_time(t_str):
                    try:
                        h, m = map(int, t_str.replace('h', '').split())
                        return today + pd.Timedelta(hours=h, minutes=m)
                    except: return None
                
                df['time_bucket'] = df['time'].apply(parse_time)
                df = df.dropna(subset=['time_bucket'])
                
                df_agg = df.groupby(['time_bucket', 'status'])['count'].sum().reset_index()
                
                # ADICIONA A ETIQUETA 'historico'
                df_agg['source'] = 'historico'
                
                df_agg.to_sql('transactions_metrics', engine, if_exists='append', index=False)
                print(f" -> Importado: {len(df_agg)} linhas.")
            except Exception as e:
                print(f"Erro ao importar {file}: {e}")

# --- LÓGICA DE DETECÇÃO (Janela Deslizante com Lag) ---
def check_anomaly(engine, status, current_count):
    try:
        # A MÁGICA: Compara com a média de 1 hora atrás, IGNORANDO os últimos 5 minutos.
        # Isso evita que o pico atual "suje" a média.
        query = text("""
            SELECT count 
            FROM transactions_metrics 
            WHERE status = :status 
            AND time_bucket BETWEEN (NOW() - INTERVAL '65 minutes') AND (NOW() - INTERVAL '5 minutes')
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"status": status})
        
        # Valores padrão para início frio (sem histórico suficiente)
        if len(df) < 5: 
            mean = 10 
            std = 5
        else:
            mean = df['count'].mean()
            std = df['count'].std()
        
        if std == 0: std = 1
        
        # Threshold: Média + 3.5x Desvio + Buffer de 10 (para não alertar volumes baixos)
        threshold = float(mean + (3.5 * std) + 10)
        
        is_anomaly = current_count > threshold
        
        msg = f"Normal (Lim: {threshold:.1f})"
        if is_anomaly:
            # Calcula porcentagem de aumento
            diff_percent = ((current_count - mean) / mean) * 100 if mean > 0 else 100
            msg = f"ALERTA DE SURTO! Valor {current_count} (Normal: ~{mean:.0f} | +{diff_percent:.0f}%)"
            
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO anomalies (event_time, metric, value_detected, threshold_limit, message, source) 
                    VALUES (NOW(), :metric, :val, :thresh, :msg, 'teste')
                """), {"metric": status, "val": int(current_count), "thresh": threshold, "msg": msg})
                conn.commit()
                
        return is_anomaly, msg, threshold
    except Exception as e: 
        print(f"Erro no check_anomaly: {e}")
        return False, str(e), 0

# --- ENDPOINT (API) ---
@app.route('/ingest', methods=['POST'])
def ingest_transaction():
    try:
        data = request.json
        status = data.get('status')
        count = int(data.get('count', 1))
        engine = get_engine()
        
        # 1. Salva Dado Bruto (com source='teste')
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO transactions_metrics (time_bucket, status, count, source) 
                VALUES (NOW(), :status, :count, 'teste')
            """), {"status": status, "count": count})
            conn.commit()
            
        # 2. Verifica Anomalia
        is_alert, message, _ = check_anomaly(engine, status, count)
        
        return jsonify({
            "status": "success", 
            "anomaly": is_alert, 
            "message": message
        })
    except Exception as e: 
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("Iniciando Monitoramento v3.0 (Lag Window)...")
    time.sleep(5)         
    init_db()             
    import_initial_csvs() 
    app.run(host='0.0.0.0', port=5000)