import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta, date
from cachetools import TTLCache
import time
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do cliente Supabase usando secrets do Streamlit Cloud
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except KeyError as e:
    st.error(f"Erro: Variável {e} não encontrada no secrets.toml. Verifique a configuração no Streamlit Cloud.")
    st.stop()

# Validar URL e chave
if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Erro: SUPABASE_URL ou SUPABASE_KEY não estão definidos.")
    st.stop()

# Inicializar cliente Supabase
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    st.error(f"Erro ao inicializar o cliente Supabase: {e}")
    st.stop()

# Configuração dos caches (TTL de 60 segundos)
cache_pcmovendpend = TTLCache(maxsize=1, ttl=60)
cache_pcpedc = TTLCache(maxsize=1, ttl=60)

# Configuração das tabelas
SUPABASE_CONFIG = [
    {
        "table_name": "PCMOVENDPEND",
        "columns": ['DTFIMOS', 'CONFERENTE', 'DTINICIOOS', 'POSICAO'],
        "date_column": "DTFIMOS",
        "cache": cache_pcmovendpend
    },
    {
        "table_name": "PCPEDC_POSICAO",
        "columns": ['DATA', 'DESCRICAO', 'L_COUNT', 'M_COUNT', 'F_COUNT'],
        "date_column": "DATA",
        "cache": cache_pcpedc
    }
]

# Função para formatar valores monetários manualmente
def formatar_valor(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# Função para buscar dados do Supabase com cache e paginação
@st.cache_data(show_spinner=False, ttl=60)
def get_data_from_supabase(_caches, data_inicial="2025-01-01", data_final="2025-05-14"):
    data = {}
    for table_config in SUPABASE_CONFIG:
        table_name = table_config["table_name"]
        cache = table_config["cache"]
        cache_key = f"{table_name}_{data_inicial}_{data_final}"
        
        if cache_key in cache:
            logger.info(f"Dados da tabela {table_name} recuperados do cache")
            data[table_name] = cache[cache_key]
            continue

        try:
            all_data = []
            offset = 0
            limit = 1000  # Limite ajustado para evitar sobrecarga

            # Formatar datas para compatibilidade
            data_inicial_str = data_inicial
            data_final_str = data_final

            while True:
                response = supabase.table(table_name).select("*").gte(table_config["date_column"], data_inicial_str).lte(table_config["date_column"], data_final_str).range(offset, offset + limit - 1).execute()
                response_data = response.data
                if not response_data:
                    logger.info(f"Finalizada a recuperação de dados da tabela {table_name}")
                    break
                all_data.extend(response_data)
                offset += limit
                logger.info(f"Recuperados {len(response_data)} registros da tabela {table_name}, total até agora: {len(all_data)}")

            if not all_data:
                logger.warning(f"Nenhum dado encontrado na tabela {table_name} para o período {data_inicial} a {data_final}")
                st.warning(f"Nenhum dado encontrado na tabela {table_name} para o período {data_inicial} a {data_final}.")
                cache[cache_key] = pd.DataFrame()
                data[table_name] = cache[cache_key]
                continue
            
            df = pd.DataFrame(all_data)
            
            # Verificar se as colunas solicitadas existem
            columns = table_config["columns"]
            missing_columns = [col for col in columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Colunas não encontradas na tabela {table_name}: {', '.join(missing_columns)}")
                st.error(f"Colunas não encontradas na tabela {table_name}: {', '.join(missing_columns)}")
                cache[cache_key] = pd.DataFrame()
                data[table_name] = cache[cache_key]
                continue
            
            # Selecionar apenas as colunas especificadas
            df = df[columns]
            
            # Verificar colunas obrigatórias
            required_columns = ['DTFIMOS', 'CONFERENTE'] if table_name == 'PCMOVENDPEND' else ['DATA', 'DESCRICAO', 'L_COUNT', 'M_COUNT']
            missing_required = [col for col in required_columns if col not in df.columns]
            if missing_required:
                logger.error(f"Colunas obrigatórias não encontradas na tabela {table_name}: {', '.join(missing_required)}")
                st.error(f"Colunas obrigatórias não encontradas na tabela {table_name}: {', '.join(missing_required)}")
                cache[cache_key] = pd.DataFrame()
                data[table_name] = cache[cache_key]
                continue
            
            # Converter colunas de data
            if table_name == 'PCMOVENDPEND':
                df['DTFIMOS'] = pd.to_datetime(df['DTFIMOS'], errors='coerce')
                df = df.dropna(subset=['DTFIMOS'])
            else:
                df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
                df = df.dropna(subset=['DATA'])
            
            # Converter colunas numéricas
            if table_name == 'PCPEDC_POSICAO':
                for col in ['L_COUNT', 'M_COUNT', 'F_COUNT']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            cache[cache_key] = df
            data[table_name] = df
            logger.info(f"Dados carregados com sucesso da tabela {table_name}: {len(df)} registros")
        except Exception as e:
            logger.error(f"Erro ao buscar dados do Supabase para a tabela {table_name}: {e}")
            st.error(f"Erro ao buscar dados do Supabase para a tabela {table_name}: {e}")
            cache[cache_key] = pd.DataFrame()
            data[table_name] = cache[cache_key]
    return data

# Função para processar dados e agrupar por dia e total
def process_data(data):
    if not data.empty:
        data['DTFIMOS'] = pd.to_datetime(data['DTFIMOS'], errors='coerce')
        data = data.dropna(subset=['DTFIMOS'])  # Remover linhas com DTFIMOS inválido
        data['DIA'] = data['DTFIMOS'].dt.date
        daily_data = data.groupby(['CONFERENTE', 'DIA']).size().reset_index(name='PEDIDOS CONFERIDOS')
        total_data = data.groupby('CONFERENTE').size().reset_index(name='PEDIDOS_TOTAL')
        return daily_data, total_data
    return pd.DataFrame(), pd.DataFrame()

# Função para realizar o reload automático a cada 1 minuto
def auto_reload():
    if 'last_reload' not in st.session_state:
        st.session_state.last_reload = time.time()
    
    current_time = time.time()
    if current_time - st.session_state.last_reload >= 60:  # 60 segundos
        st.session_state.last_reload = current_time
        st.cache_data.clear()  # Limpar o cache para forçar nova busca
        st.rerun()  # Forçar reload da página

def main():
    # Chamar auto_reload para verificar se precisa atualizar
    auto_reload()

    # Custom CSS para estilização responsiva
    st.markdown("""
    <style>
        /* General table styling */
        .ranking-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: clamp(14px, 2vw, 16px);
        }
        .ranking-table th, .ranking-table td {
            padding: 12px;
            text-align: center;
            border: 1px solid #ddd;
        }
        .ranking-table tr:nth-child(even) {
            background-color: #f00000;
        }
        .ranking-table tr:hover {
            background-color: #ddd;
        }

        /* Card styling */
        .card {
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            padding: 10px;
            background-color: #007bff;
            border-radius: 10px;
            box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.1);
            width: 100%;
            text-align: center;
            min-height: 150px;
            color: white;
            margin-bottom: 20px;
            transition: all 0.3s ease;
        }
        .card-content {
            width: 100%;
        }
        .title {
            font-size: clamp(14px, 2vw, 16px);
            font-weight: bold;
            margin-top: 10px;
        }
        .number {
            font-size: clamp(16px, 2.5vw, 18px);
            font-weight: bold;
            margin: 4px 0;
        }

        /* Total container */
        .total-container {
            display: flex;
            flex-direction: column;
            gap: 20px;
            text-align: center;
            align-items: center;
            width: 100%;
            margin-top: 20px;
        }
        .total-item {
            font-size: clamp(18px, 3vw, 22px);
            font-weight: bold;
            color: #fff;
        }
        .total-paragrafo {
            font-size: clamp(14px, 2vw, 16px);
        }
        .total-item-final {
            font-size: clamp(14px, 2vw, 16px);
        }

        /* Scrollable table */
        .scrollable-table {
            max-height: 400px;
            overflow-y: auto;
            display: block;
            border: 1px solid #ddd;
            width: 100%;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }

        /* Responsive adjustments */
        @media (max-width: 768px) {
            .card {
                min-height: 120px;
            }
            .total-container {
                gap: 10px;
            }
            .ranking-table th, .ranking-table td {
                padding: 8px;
                font-size: 14px;
            }
        }

        @media (max-width: 480px) {
            .card {
                min-height: 100px;
            }
            .number {
                font-size: 14px;
            }
            .title {
                font-size: 12px;
            }
            .total-item {
                font-size: 16px;
            }
            .total-paragrafo {
                font-size: 12px;
            }
        }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<h1>Relatório de Pedidos</h1>", unsafe_allow_html=True)

    # Botão para limpar cache manualmente
    if st.button("Limpar Cache"):
        st.cache_data.clear()
        st.rerun()

    # Definir período padrão (1º de janeiro até 14/05/2025)
    data_inicial_default = "2025-01-01"
    data_final_default = "2025-05-14"

    # Buscar dados do Supabase com cache
    caches = {config["table_name"]: config["cache"] for config in SUPABASE_CONFIG}
    with st.spinner("Carregando dados..."):
        data = get_data_from_supabase(caches, data_inicial_default, data_final_default)
    data_1 = data.get('PCMOVENDPEND', pd.DataFrame())
    data_2 = data.get('PCPEDC_POSICAO', pd.DataFrame())

    # Ajustar para a data atual: 14/05/2025
    hoje = date(2025, 5, 14)
    inicio_semana = hoje - timedelta(days=hoje.weekday())
    inicio_mes = hoje.replace(day=1)

    if not data_1.empty or not data_2.empty:
        daily_data, total_data = process_data(data_1)
        logger.info(f"Colunas de daily_data: {daily_data.columns.tolist()}")
        if not daily_data.empty:
            logger.info(f"Primeiras linhas de daily_data: {daily_data.head().to_dict()}")

        if not data_2.empty:
            data_2['DATA'] = pd.to_datetime(data_2['DATA'], errors='coerce')
            data_2 = data_2.dropna(subset=['DATA'])  # Remover linhas com DATA inválida
            rotas_desejadas = ["GRANDE VITORIA", "REGIÃO SUL", "REGIAO NORTE", "BR 262", "EXTREMO SUL", "EXTREMO NORTE", "EXTREMO CENTRO/ES"]

            total_liberados = data_2['L_COUNT'].sum()
            total_montados = data_2['M_COUNT'].sum()

            st.markdown("<h3>Regiões</h3>", unsafe_allow_html=True)
            rotas_selecionadas = st.multiselect("Selecione as Rotas", rotas_desejadas, default=rotas_desejadas)
            data_filtrada = data_2[data_2['DESCRICAO'].isin(rotas_selecionadas)]

            data_aggregated = data_filtrada.groupby(['DESCRICAO']).agg(
                pedidos_liberados=('L_COUNT', 'sum'),
                pedidos_montados=('M_COUNT', 'sum')
            ).reset_index()

        # Calcular totais com validação de coluna
        total_dia = daily_data[daily_data['DIA'] == hoje]['PEDIDOS CONFERIDOS'].sum() if not daily_data.empty and 'PEDIDOS CONFERIDOS' in daily_data.columns else 0
        total_semana = daily_data[(daily_data['DIA'] >= inicio_semana) & (daily_data['DIA'] <= hoje)]['PEDIDOS CONFERIDOS'].sum() if not daily_data.empty and 'PEDIDOS CONFERIDOS' in daily_data.columns else 0
        total_mes = daily_data[(daily_data['DIA'] >= inicio_mes) & (daily_data['DIA'] <= hoje)]['PEDIDOS CONFERIDOS'].sum() if not daily_data.empty and 'PEDIDOS CONFERIDOS' in daily_data.columns else 0

        # Duas colunas lado a lado
        col1, col2 = st.columns([1, 1])

        with col1:
            if not data_2.empty:
                st.markdown("### Pedidos por Rota")
                num_colunas = 3 if st.session_state.get('screen_width', 1000) > 768 else 1
                cols = st.columns(num_colunas)

                colors = {
                    "GRANDE VITORIA": "#FF6347",
                    "REGIÃO SUL": "#32CD32",
                    "REGIAO NORTE": "#000080",
                    "BR 262": "#6A5ACD",
                    "EXTREMO SUL": "#FF69B4",
                    "EXTREMO NORTE": "#20B2AA",
                    "EXTREMO CENTRO/ES": "#FF4500",
                }

                for index, row in data_aggregated.iterrows():
                    if index >= len(cols) * 3:  # Limitar para evitar erros de índice
                        break
                    rota_nome = row['DESCRICAO']
                    pedidos_liberados = row['pedidos_liberados']
                    pedidos_montados = row['pedidos_montados']
                    col = cols[index % num_colunas]
                    with col:
                        st.markdown(f"""
                            <div class="card" style="background-color: {colors.get(rota_nome, '#007bff')}">
                                <span class="title">{rota_nome}</span><br>
                                <div class="card-content">
                                    <div class="card-item" style="display: flex; align-items: center; justify-content: space-around">
                                        <img src="https://cdn-icons-png.flaticon.com/512/5629/5629260.png" width="30" style="margin-right: 5px;">
                                        <p style="margin: 0px; font-weight: bold;">LIBERADOS:</p>
                                        <div class="number">{int(pedidos_liberados)}</div>
                                    </div>
                                    <div class="card-item" style="display: flex; align-items: center; justify-content: space-around">
                                        <img src="https://cdn-icons-png.flaticon.com/512/9964/9964349.png" width="30" style="margin-right: 5px;">
                                        <p style="margin: 0px; font-weight: bold;">MONTADOS:</p>
                                        <div class="number">{int(pedidos_montados)}</div>
                                    </div>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.warning("Nenhum dado disponível para exibir pedidos por rota.")

        with col2:
            st.markdown("### Pedidos Conferidos por Funcionário")
            data_inicial = st.date_input("Data Inicial", value=hoje, key="data_inicial")
            data_final = st.date_input("Data Final", value=hoje, key="data_final")

            if data_inicial > data_final:
                st.error("A data inicial não pode ser maior que a data final.")
                return

            filtered_data = daily_data[(daily_data['DIA'] >= data_inicial) & (daily_data['DIA'] <= data_final)] if not daily_data.empty else pd.DataFrame()
            if not filtered_data.empty:
                filtered_data_sorted = filtered_data.sort_values(by=['DIA', 'PEDIDOS CONFERIDOS'], ascending=[True, False]).reset_index(drop=True)
                # Format DIA as string to avoid serialization issues
                filtered_data_sorted['DIA'] = filtered_data_sorted['DIA'].astype(str)
                st.markdown('<div class="scrollable-table">' + filtered_data_sorted.to_html(index=False, escape=False, classes="ranking-table") + '</div>', unsafe_allow_html=True)
            else:
                st.warning("Nenhum dado encontrado para o intervalo de datas selecionado.")

        # Relatório de Pedidos
        st.markdown("<h2>Relatório de Pedidos</h2>", unsafe_allow_html=True)
        st.markdown(f"""
            <div class="total-container">
                <div class="total-item">
                    <img src="https://cdn-icons-png.flaticon.com/512/10995/10995680.png" width="35" style="margin-right: 5px;">
                    <span class="total-paragrafo">TOTAL LIBERADOS:</span>
                    <span class="total-numero-conf">{int(total_liberados)}</span>
                </div>
                <div class="total-item">
                    <img src="https://cdn-icons-png.flaticon.com/512/976/976438.png" width="35" style="margin-right: 5px;">
                    <span class="total-paragrafo">TOTAL MONTADOS:</span>
                    <span class="total-numero-conf">{int(total_montados)}</span>
                </div>
                <div class="total-item">
                    <img src="https://cdn-icons-png.flaticon.com/512/5220/5220625.png" width="35" style="margin-right: 5px;">
                    <span class="total-paragrafo">CONF DIÁRIA:</span>
                    <span class="total-numero-conf">{int(total_dia)}</span>
                    <span class="total-item-final">PEDIDOS</span>
                </div>
                <div class="total-item">
                    <img src="https://cdn-icons-png.flaticon.com/512/391/391175.png" width="35" style="margin-right: 5px;">
                    <span class="total-paragrafo">CONF SEMANAL:</span>
                    <span class="total-numero-conf">{int(total_semana)}</span>
                    <span class="total-item-final">PEDIDOS</span>
                </div>
                <div class="total-item">
                    <img src="https://cdn-icons-png.flaticon.com/512/353/353267.png" width="35" style="margin-right: 5px;">
                    <span class="total-paragrafo">CONF MENSAL:</span>
                    <span class="total-numero-conf">{int(total_mes)}</span>
                    <span class="total-item-final">PEDIDOS</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.warning("Nenhum dado disponível para exibição.")

if __name__ == "__main__":
    main()
