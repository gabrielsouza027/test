import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import locale
import plotly.express as px
import logging
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_exponential

# Configurar logging
logging.basicConfig(level=logging.DEBUG)  # Aumentado para DEBUG para mais detalhes
logger = logging.getLogger(__name__)

# Definir o local para a formatação monetária
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    logger.warning("Locale 'pt_BR.UTF-8' não disponível, usando padrão.")
    locale.setlocale(locale.LC_ALL, '')

# Configuração das URLs e tabelas do Supabase
# Removido filtros restritivos temporariamente para testar
SUPABASE_TABLES = [
    {
        "table_name": "PCPEDC",
        "url": f"{st.secrets['SUPABASE_URL']}/rest/v1/PCPEDC?select=*"
    },
    # Adicione mais tabelas aqui, se necessário
    # {
    #     "table_name": "VWSOMELIER",
    #     "url": f"{st.secrets['SUPABASE_URL']}/rest/v1/VWSOMELIER?select=*"
    # },
]

# Cabeçalhos comuns para todas as requisições
def get_headers():
    try:
        return {
            "apikey": st.secrets["SUPABASE_KEY"],
            "Authorization": f"Bearer {st.secrets['SUPABASE_KEY']}",
            "Accept": "application/json"
        }
    except KeyError as e:
        st.error(f"Erro: Variável {e} não encontrada no secrets.toml. Verifique a configuração no Streamlit Cloud.")
        st.stop()

# Função para buscar dados de uma tabela com retry
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_table_data(table, page_size, offset):
    table_name = table["table_name"]
    url = table["url"]
    headers = get_headers()
    headers["Range"] = f"{offset}-{offset + page_size - 1}"
    
    try:
        logger.debug(f"Fazendo requisição para {url} com offset {offset}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Resposta recebida: {len(data)} registros, status {response.status_code}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao buscar dados da tabela {table_name}, offset {offset}: {e}, resposta: {response.text if 'response' in locals() else 'sem resposta'}")
        raise

# Função para carregar dados do Supabase com cache, paginação e paralelismo
@st.cache_data(show_spinner=False, ttl=3600)
def carregar_dados(_cache_key=datetime.now().strftime('%Y%m%d%H%M')):  # Cache_key para invalidar periodicamente
    all_data = []
    page_size = 500  # Mantido para respostas rápidas

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for table in SUPABASE_TABLES:
                table_name = table["table_name"]
                logger.info(f"Iniciando carregamento da tabela {table_name}")
                
                offset = 0
                while True:
                    futures.append(executor.submit(fetch_table_data, table, page_size, offset))
                    offset += page_size
                    if offset >= page_size * 10:  # Limite ajustável
                        logger.info(f"Limite de páginas atingido para {table_name}")
                        break

            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    if data:
                        all_data.extend(data)
                except Exception as e:
                    logger.error(f"Erro em uma requisição: {e}")
                    continue

        if all_data:
            data = pd.DataFrame(all_data)
            logger.debug(f"Colunas retornadas: {data.columns.tolist()}")
            required_columns = ['PVENDA', 'QT', 'CODFILIAL', 'DATA_PEDIDO', 'NUMPED']
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                logger.error(f"Colunas ausentes nos dados: {missing_columns}")
                st.error(f"Colunas ausentes nos dados retornados pela API: {missing_columns}. Verifique a estrutura da tabela {SUPABASE_TABLES[0]['table_name']}.")
                return pd.DataFrame()

            data['PVENDA'] = pd.to_numeric(data['PVENDA'], errors='coerce').fillna(0).astype('float32')
            data['QT'] = pd.to_numeric(data['QT'], errors='coerce').fillna(0).astype('int32')
            data['CODFILIAL'] = data['CODFILIAL'].astype(str)
            data['NUMPED'] = data['NUMPED'].astype(str)
            data['VLTOTAL'] = data['PVENDA'] * data['QT']
            
            # Aplicar filtro de filiais no Python (já que removido da URL)
            data = data[data['CODFILIAL'].isin(['1', '2'])].copy()
            
            data['DATA_PEDIDO'] = pd.to_datetime(data['DATA_PEDIDO'], errors='coerce')
            if data['DATA_PEDIDO'].isnull().any():
                logger.warning("Valores inválidos encontrados na coluna 'DATA_PEDIDO'.")
                st.warning("Valores inválidos encontrados na coluna 'DATA_PEDIDO'. Filtrando registros inválidos.")
                data = data.dropna(subset=['DATA_PEDIDO'])
            
            logger.info(f"Dados carregados com sucesso: {len(data)} registros")
        else:
            logger.error("Nenhum dado retornado pela API. Possíveis causas: tabela vazia, filtros incorretos ou erro de autenticação.")
            st.error("Nenhum dado retornado pela API. Verifique: 1) Se a tabela PCPEDC contém dados; 2) Se as chaves SUPABASE_URL e SUPABASE_KEY estão corretas; 3) Se a tabela tem as colunas esperadas (PVENDA, QT, CODFILIAL, DATA_PEDIDO, NUMPED).")
            data = pd.DataFrame()

    except Exception as e:
        logger.error(f"Erro geral ao processar dados: {e}")
        st.error(f"Erro ao processar dados: {e}. Verifique a configuração do Supabase e a conexão com a API.")
        data = pd.DataFrame()

    return data

# Funções de cálculo
def calcular_faturamento(data, hoje, ontem, semana_inicial, semana_passada_inicial):
    faturamento_hoje = data[data['DATA_PEDIDO'] == hoje]['VLTOTAL'].sum()
    faturamento_ontem = data[data['DATA_PEDIDO'] == ontem]['VLTOTAL'].sum()
    faturamento_semanal_atual = data[(data['DATA_PEDIDO'] >= semana_inicial) & (data['DATA_PEDIDO'] <= hoje)]['VLTOTAL'].sum()
    faturamento_semanal_passada = data[(data['DATA_PEDIDO'] >= semana_passada_inicial) & (data['DATA_PEDIDO'] < semana_inicial)]['VLTOTAL'].sum()
    return faturamento_hoje, faturamento_ontem, faturamento_semanal_atual, faturamento_semanal_passada

def calcular_quantidade_pedidos(data, hoje, ontem, semana_inicial, semana_passada_inicial):
    pedidos_hoje = data[data['DATA_PEDIDO'] == hoje]['NUMPED'].nunique()
    pedidos_ontem = data[data['DATA_PEDIDO'] == ontem]['NUMPED'].nunique()
    pedidos_semanal_atual = data[(data['DATA_PEDIDO'] >= semana_inicial) & (data['DATA_PEDIDO'] <= hoje)]['NUMPED'].nunique()
    pedidos_semanal_passada = data[(data['DATA_PEDIDO'] >= semana_passada_inicial) & (data['DATA_PEDIDO'] < semana_inicial)]['NUMPED'].nunique()
    return pedidos_hoje, pedidos_ontem, pedidos_semanal_atual, pedidos_semanal_passada

def calcular_comparativos(data, hoje, mes_atual, ano_atual):
    mes_anterior = mes_atual - 1 if mes_atual > 1 else 12
    ano_anterior = ano_atual if mes_atual > 1 else ano_atual - 1
    faturamento_mes_atual = data[(data['DATA_PEDIDO'].dt.month == mes_atual) & (data['DATA_PEDIDO'].dt.year == ano_atual)]['VLTOTAL'].sum()
    pedidos_mes_atual = data[(data['DATA_PEDIDO'].dt.month == mes_atual) & (data['DATA_PEDIDO'].dt.year == ano_atual)]['NUMPED'].nunique()
    faturamento_mes_anterior = data[(data['DATA_PEDIDO'].dt.month == mes_anterior) & (data['DATA_PEDIDO'].dt.year == ano_anterior)]['VLTOTAL'].sum()
    pedidos_mes_anterior = data[(data['DATA_PEDIDO'].dt.month == mes_anterior) & (data['DATA_PEDIDO'].dt.year == ano_anterior)]['NUMPED'].nunique()
    return faturamento_mes_atual, faturamento_mes_anterior, pedidos_mes_atual, pedidos_mes_anterior

def formatar_valor(valor):
    try:
        return locale.currency(valor, grouping=True, symbol=True)
    except Exception:
        return f"R$ {valor:,.2f}"

def main():
    st.markdown("""
    <style>
        .st-emotion-cache-1ibsh2c {
            width: 100%;
            padding: 0rem 1rem 0rem;
            max-width: initial;
            min-width: auto;
        }
        .st-column {
            display: flex;
            justify-content: center;
            align-items: center;
        }
        .card-container {
            display: flex;
            align-items: center;
            background-color: #302d2d;
            padding: 10px;
            border-radius: 8px;
            margin-bottom: 10px;
            color: white;
            flex-direction: column;
            text-align: center;
        }
        .card-container img {
            width: 51px;
            height: 54px;
            margin-bottom: 5px;
        }
        .number {
            font-size: 20px;
            font-weight: bold;
            margin-top: 5px;
        }
    </style>
    """, unsafe_allow_html=True)

    st.title('Dashboard de Faturamento')
    st.markdown("### Resumo de Vendas")

    # Carregar dados com cache
    with st.spinner("Carregando dados do Supabase..."):
        data = carregar_dados()
    
    if data.empty:
        st.error("Não foi possível carregar os dados. Verifique as configurações da API, a existência de dados na tabela PCPEDC, ou tente novamente.")
        return

    col1, col2 = st.columns(2)
    with col1:
        filial_1 = st.checkbox("Filial 1", value=True)
    with col2:
        filial_2 = st.checkbox("Filial 2", value=True)

    # Definir filiais selecionadas
    filiais_selecionadas = []
    if filial_1:
        filiais_selecionadas.append('1')
    if filial_2:
        filiais_selecionadas.append('2')

    # Verificar se pelo menos uma filial está selecionada
    if not filiais_selecionadas:
        st.warning("Por favor, selecione pelo menos uma filial para exibir os dados.")
        return

    # Filtrar dados com base nas filiais selecionadas
    data_filtrada = data[data['CODFILIAL'].isin(filiais_selecionadas)].copy()

    hoje = pd.to_datetime('2025-05-13').normalize()  # Data fixa conforme contexto
    ontem = hoje - timedelta(days=1)
    semana_inicial = hoje - timedelta(days=hoje.weekday())
    semana_passada_inicial = semana_inicial - timedelta(days=7)

    faturamento_hoje, faturamento_ontem, faturamento_semanal_atual, faturamento_semanal_passada = calcular_faturamento(data_filtrada, hoje, ontem, semana_inicial, semana_passada_inicial)
    pedidos_hoje, pedidos_ontem, pedidos_semanal_atual, pedidos_semanal_passada = calcular_quantidade_pedidos(data_filtrada, hoje, ontem, semana_inicial, semana_passada_inicial)

    mes_atual = hoje.month
    ano_atual = hoje.year
    faturamento_mes_atual, faturamento_mes_anterior, pedidos_mes_atual, pedidos_mes_anterior = calcular_comparativos(data_filtrada, hoje, mes_atual, ano_atual)

    col1, col2, col3, col4, col5 = st.columns(5)

    def calcular_variacao(atual, anterior):
        if anterior == 0:
            return 100 if atual > 0 else 0
        return ((atual - anterior) / anterior) * 100
    
    def icone_variacao(valor):
        if valor > 0:
            return f"<span style='color: green;'>▲ {valor:.2f}%</span>"
        elif valor < 0:
            return f"<span style='color: red;'>▼ {valor:.2f}%</span>"
        else:
            return f"{valor:.2f}%"

    var_faturamento_mes = calcular_variacao(faturamento_mes_atual, faturamento_mes_anterior)
    var_pedidos_mes = calcular_variacao(pedidos_mes_atual, pedidos_mes_anterior)
    var_faturamento_hoje = calcular_variacao(faturamento_hoje, faturamento_ontem)
    var_pedidos_hoje = calcular_variacao(pedidos_hoje, pedidos_ontem)
    var_faturamento_semananterior = calcular_variacao(faturamento_semanal_atual, faturamento_semanal_passada)

    def grafico_pizza_variacao(labels, valores, titulo):
        fig = px.pie(
            names=labels,
            values=valores,
            title=titulo,
            hole=0.4,
            color=labels,
            color_discrete_map={"Positivo": "green", "Negativo": "red"}
        )
        fig.update_layout(margin=dict(t=30, b=30, l=30, r=30))
        return fig

    with col1:
        st.markdown(f"""
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/2460/2460494.png" alt="Ícone Hoje">
                <span>Hoje:</span> 
                <div class="number">{formatar_valor(faturamento_hoje)}</div>
                <small>Variação: {icone_variacao(var_faturamento_hoje)}</small>
            </div>
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/3703/3703896.png" alt="Ícone Ontem">
                <span>Ontem:</span> 
                <div class="number">{formatar_valor(faturamento_ontem)}</div>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/4435/4435153.png" alt="Ícone Semana Atual">
                <span>Semana Atual:</span> 
                <div class="number">{formatar_valor(faturamento_semanal_atual)}</div>
                <small>Variação: {icone_variacao(var_faturamento_semananterior)}</small>
            </div>
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/4435/4435153.png" alt="Ícone Semana Passada">
                <span>Semana Passada:</span> 
                <div class="number">{formatar_valor(faturamento_semanal_passada)}</div>
            </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/10535/10535844.png" alt="Ícone Mês Atual">
                <span>Mês Atual:</span> 
                <div class="number">{formatar_valor(faturamento_mes_atual)}</div>
                <small>Variação: {icone_variacao(var_faturamento_mes)}</small>
            </div>
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/584/584052.png" alt="Ícone Mês Anterior">
                <span>Mês Anterior:</span> 
                <div class="number">{formatar_valor(faturamento_mes_anterior)}</div>
            </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/6632/6632848.png" alt="Ícone Pedidos Mês Atual">
                <span>Pedidos Mês Atual:</span> 
                <div class="number">{pedidos_mes_atual}</div>
                <small>Variação: {icone_variacao(var_pedidos_mes)}</small>
            </div>
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/925/925049.png" alt="Ícone Pedidos Mês Anterior">
                <span>Pedidos Mês Anterior:</span> 
                <div class="number">{pedidos_mes_anterior}</div>
            </div>
        """, unsafe_allow_html=True)

    with col5:
        st.markdown(f"""
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/14018/14018701.png" alt="Ícone Pedidos Hoje">
                <span>Pedidos Hoje:</span> 
                <div class="number">{pedidos_hoje}</div>
                <small>Variação: {icone_variacao(var_pedidos_hoje)}</small>
            </div>
            <div class="card-container">
                <img src="https://cdn-icons-png.flaticon.com/512/5220/5220625.png" alt="Ícone Pedidos Ontem">
                <span>Pedidos Ontem:</span> 
                <div class="number">{pedidos_ontem}</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.plotly_chart(grafico_pizza_variacao(["Hoje", "Ontem"], [abs(faturamento_hoje), abs(faturamento_ontem)], "Variação de Faturamento (Hoje x Ontem)"), use_container_width=True)
    with col2:
        st.plotly_chart(grafico_pizza_variacao(["Semana Atual", "Semana Passada"], [abs(faturamento_semanal_atual), abs(faturamento_semanal_passada)], "Variação de Faturamento (Semana)"), use_container_width=True)
    with col3:
        st.plotly_chart(grafico_pizza_variacao(["Mês Atual", "Mês Anterior"], [abs(faturamento_mes_atual), abs(faturamento_mes_anterior)], "Variação de Faturamento (Mês)"), use_container_width=True)
    with col4:
        st.plotly_chart(grafico_pizza_variacao(["Pedidos Mês Atual", "Pedidos Mês Passado"], [abs(pedidos_mes_atual), abs(pedidos_mes_anterior)], "Variação de Pedidos (Mês)"), use_container_width=True)
    with col5:
        st.plotly_chart(grafico_pizza_variacao(["Pedidos Hoje", "Pedidos Ontem"], [abs(pedidos_hoje), abs(pedidos_ontem)], "Variação de Pedidos (Hoje x Ontem)"), use_container_width=True)

    # Gráfico de linhas com seletores de data
    st.markdown("---")
    st.subheader("Comparação de Vendas por Mês e Ano")

    # Seletores de data
    col_data1, col2 = st.columns(2)
    with col_data1:
        default_inicial = data_filtrada['DATA_PEDIDO'].min() if not data_filtrada.empty else pd.to_datetime('2024-01-01')
        data_inicial = st.date_input("Data Inicial", value=default_inicial, min_value=data_filtrada['DATA_PEDIDO'].min() if not data_filtrada.empty else None, max_value=data_filtrada['DATA_PEDIDO'].max() if not data_filtrada.empty else None)
    with col2:
        data_final = st.date_input("Data Final", value=hoje, min_value=data_filtrada['DATA_PEDIDO'].min() if not data_filtrada.empty else None, max_value=data_filtrada['DATA_PEDIDO'].max() if not data_filtrada.empty else None)

    if data_inicial > data_final:
        st.error("A Data Inicial não pode ser maior que a Data Final.")
        return

    # Filtrar dados pelo período selecionado
    df_periodo = data_filtrada[(data_filtrada['DATA_PEDIDO'] >= pd.to_datetime(data_inicial)) & 
                               (data_filtrada['DATA_PEDIDO'] <= pd.to_datetime(data_final))].copy()

    if df_periodo.empty:
        st.warning("Nenhum dado disponível para o período selecionado.")
        return

    # Adicionar colunas de ano e mês
    df_periodo['Ano'] = df_periodo['DATA_PEDIDO'].dt.year.astype(str)
    df_periodo['Mês'] = df_periodo['DATA_PEDIDO'].dt.month

    # Agrupar por ano e mês
    vendas_por_mes_ano = df_periodo.groupby(['Ano', 'Mês']).agg(
        Valor_Total_Vendido=('VLTOTAL', 'sum')
    ).reset_index()

    # Criar gráfico de linhas com uma linha por ano
    fig = px.line(vendas_por_mes_ano, x='Mês', y='Valor_Total_Vendido', color='Ano',
                  title=f'Vendas por Mês ({data_inicial} a {data_final})',
                  labels={'Mês': 'Mês', 'Valor_Total_Vendido': 'Valor Total Vendido (R$)', 'Ano': 'Ano'},
                  markers=True)

    # Ajustes visuais
    fig.update_layout(
        title_font_size=20,
        xaxis_title_font_size=16,
        yaxis_title_font_size=16,
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14,
        xaxis_tickangle=-45,
        xaxis=dict(tickmode='array', tickvals=list(range(1, 13)), ticktext=['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'])
    )

    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
