import streamlit as st
import polars as pl
import pandas as pd
import requests
from datetime import datetime, timedelta
import locale
import plotly.express as px
import logging
from concurrent.futures import ThreadPoolExecutor
import os
import pickle

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Definir o local para a formatação monetária
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    logger.warning("Locale 'pt_BR.UTF-8' não disponível, usando padrão.")
    locale.setlocale(locale.LC_ALL, '')

# Configuração das URLs e tabelas do Supabase
SUPABASE_TABLES = [
    {
        "table_name": "PCPEDC",
        "url": f"{st.secrets['SUPABASE_URL']}/rest/v1/PCPEDC?select=*"
    },
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

# Função para carregar dados de uma única tabela com suporte a filtros de data
def fetch_table_data(table, page_size=150023, last_fetched=None):
    table_name = table["table_name"]
    url = table["url"]
    if last_fetched:
        url += f"&DATA_PEDIDO=gt.{last_fetched.strftime('%Y-%m-%dT%H:%M:%S')}"
    logger.info(f"Carregando dados da tabela {table_name} com URL: {url}")
    all_data = []

    offset = 0
    while True:
        headers = get_headers()
        headers["Range"] = f"{offset}-{offset + page_size - 1}"
        
        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            data = response.json()
            if not data:
                logger.info(f"Finalizada a recuperação de dados da tabela {table_name}")
                break
            all_data.extend(data)
            offset += page_size
            logger.info(f"Recuperados {len(data)} registros da tabela {table_name}, total até agora: {len(all_data)}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar dados da tabela {table_name}: {e}")
            return []

    return all_data

# Função para salvar e carregar cache em arquivo
def save_cache(data, filename="cache.pkl"):
    try:
        with open(filename, "wb") as f:
            pickle.dump(data, f)
        logger.info(f"Cache salvo em {filename}")
    except Exception as e:
        logger.warning(f"Erro ao salvar cache: {e}")

def load_cache(filename="cache.pkl"):
    try:
        if os.path.exists(filename):
            with open(filename, "rb") as f:
                data = pickle.load(f)
            logger.info(f"Cache carregado de {filename}")
            return data
        else:
            logger.info("Nenhum cache encontrado")
            return None
    except Exception as e:
        logger.warning(f"Erro ao carregar cache: {e}")
        return None

# Função para carregar dados do Supabase com cache e paralelismo
@st.cache_data(show_spinner=False, ttl=900)
def carregar_dados(_last_fetched=None):
    try:
        # Carregar cache existente
        cached_data = load_cache()
        cached_df = pl.DataFrame(cached_data) if cached_data else pl.DataFrame()
        logger.info(f"Colunas no cache: {cached_df.columns} (total: {len(cached_df.columns)}), tipos: {dict(cached_df.schema)}")

        # Carregar dados novos em paralelo usando ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(lambda table: fetch_table_data(table, last_fetched=_last_fetched), SUPABASE_TABLES))

        # Combinar todos os dados novos
        all_data = []
        for result in results:
            all_data.extend(result)

        if not all_data and cached_data is None:
            logger.warning("Nenhum dado retornado pela API e nenhum cache disponível")
            st.error("Nenhum dado retornado pela API.")
            return pl.DataFrame()

        # Converter novos dados para Polars DataFrame
        new_df = pl.DataFrame(all_data) if all_data else pl.DataFrame()
        logger.info(f"Colunas nos novos dados: {new_df.columns} (total: {len(new_df.columns)}), tipos: {dict(new_df.schema)}")

        # Definir o schema esperado
        expected_columns = ['PVENDA', 'QT', 'CODFILIAL', 'DATA_PEDIDO', 'NUMPED', 'VLTOTAL']

        # Alinhar schemas
        if not new_df.is_empty():
            for col in expected_columns:
                if col not in new_df.columns:
                    new_df = new_df.with_columns(pl.lit(None).alias(col))
            new_df = new_df.with_columns([
                pl.col('PVENDA').cast(pl.Float64).fill_null(0),
                pl.col('QT').cast(pl.Float64).fill_null(0),
                (pl.col('PVENDA').fill_null(0) * pl.col('QT').fill_null(0)).alias('VLTOTAL').fill_null(0)
            ])

        if not cached_df.is_empty():
            for col in expected_columns:
                if col not in cached_df.columns:
                    cached_df = cached_df.with_columns(pl.lit(None).alias(col))
            cached_df = cached_df.with_columns([
                pl.col('PVENDA').cast(pl.Float64).fill_null(0),
                pl.col('QT').cast(pl.Float64).fill_null(0),
                (pl.col('PVENDA').fill_null(0) * pl.col('QT').fill_null(0)).alias('VLTOTAL').fill_null(0)
            ])

        # Forçar o mesmo conjunto de colunas em ambos os DataFrames
        if not cached_df.is_empty() and not new_df.is_empty():
            common_columns = list(set(cached_df.columns) & set(new_df.columns))
            logger.info(f"Colunas comuns: {common_columns}")
            cached_df = cached_df.select(common_columns)
            new_df = new_df.select(common_columns)
        elif not cached_df.is_empty():
            new_df = cached_df.clone().clear()
        elif not new_df.is_empty():
            cached_df = new_df.clone().clear()

        # Combinar dados
        combined_df = pl.concat([cached_df, new_df]).unique(subset=["NUMPED"])
        logger.info(f"Colunas após concatenação: {combined_df.columns} (total: {len(combined_df.columns)}), tipos: {dict(combined_df.schema)}")

        # Verificar se as colunas necessárias existem
        required_columns = ['PVENDA', 'QT', 'CODFILIAL', 'DATA_PEDIDO', 'NUMPED']
        missing_columns = [col for col in required_columns if col not in combined_df.columns]
        if missing_columns:
            logger.error(f"Colunas ausentes nos dados: {missing_columns}")
            st.error(f"Colunas ausentes nos dados retornados pela API: {missing_columns}")
            return pl.DataFrame()

        # Processar DATA_PEDIDO com múltiplos formatos
        if not combined_df['DATA_PEDIDO'].is_empty():
            # Log alguns valores para debug
            sample_dates = combined_df['DATA_PEDIDO'].head(5).to_list()
            logger.info(f"Amostra de DATA_PEDIDO: {sample_dates}")
            combined_df = combined_df.with_columns([
                pl.col('PVENDA').cast(pl.Float64).fill_null(0),
                pl.col('QT').cast(pl.Float64).fill_null(0),
                pl.col('CODFILIAL').cast(pl.Utf8),
                pl.col('NUMPED').cast(pl.Utf8),
                pl.when(pl.col('DATA_PEDIDO').str.lengths() > 0)
                .then(pl.col('DATA_PEDIDO').str.to_datetime(
                    [ "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d" ],
                    strict=False
                ))
                .otherwise(None).alias('DATA_PEDIDO')
            ])

        # Recalcular VLTOTAL para consistência
        combined_df = combined_df.with_columns((pl.col('PVENDA') * pl.col('QT')).alias('VLTOTAL').fill_null(0))

        # Filtrar apenas filiais 1 e 2
        combined_df = combined_df.filter(pl.col('CODFILIAL').is_in(['1', '2']))

        # Remover registros com DATA_PEDIDO nula
        if combined_df['DATA_PEDIDO'].is_null().any():
            logger.warning("Valores inválidos encontrados na coluna 'DATA_PEDIDO'.")
            st.warning("Valores inválidos encontrados na coluna 'DATA_PEDIDO'. Filtrando registros inválidos.")
            combined_df = combined_df.filter(pl.col('DATA_PEDIDO').is_not_null())

        # Salvar cache atualizado
        if not combined_df.is_empty():
            save_cache(combined_df.to_dicts())

        logger.info(f"Dados carregados com sucesso: {len(combined_df)} registros")
        return combined_df

    except Exception as e:
        logger.error(f"Erro geral ao processar dados: {e}")
        st.error(f"Erro ao processar dados: {e}")
        return pl.DataFrame()

# Funções de cálculo ajustadas para Polars
def calcular_faturamento(data, hoje, ontem, semana_inicial, semana_passada_inicial):
    faturamento_hoje = data.filter(pl.col('DATA_PEDIDO') == hoje)['VLTOTAL'].sum()
    faturamento_ontem = data.filter(pl.col('DATA_PEDIDO') == ontem)['VLTOTAL'].sum()
    faturamento_semanal_atual = data.filter((pl.col('DATA_PEDIDO') >= semana_inicial) & (pl.col('DATA_PEDIDO') <= hoje))['VLTOTAL'].sum()
    faturamento_semanal_passada = data.filter((pl.col('DATA_PEDIDO') >= semana_passada_inicial) & (pl.col('DATA_PEDIDO') < semana_inicial))['VLTOTAL'].sum()
    return faturamento_hoje, faturamento_ontem, faturamento_semanal_atual, faturamento_semanal_passada

def calcular_quantidade_pedidos(data, hoje, ontem, semana_inicial, semana_passada_inicial):
    pedidos_hoje = data.filter(pl.col('DATA_PEDIDO') == hoje)['NUMPED'].n_unique()
    pedidos_ontem = data.filter(pl.col('DATA_PEDIDO') == ontem)['NUMPED'].n_unique()
    pedidos_semanal_atual = data.filter((pl.col('DATA_PEDIDO') >= semana_inicial) & (pl.col('DATA_PEDIDO') <= hoje))['NUMPED'].n_unique()
    pedidos_semanal_passada = data.filter((pl.col('DATA_PEDIDO') >= semana_passada_inicial) & (pl.col('DATA_PEDIDO') < semana_inicial))['NUMPED'].n_unique()
    return pedidos_hoje, pedidos_ontem, pedidos_semanal_atual, pedidos_semanal_passada

def calcular_comparativos(data, hoje, mes_atual, ano_atual):
    mes_anterior = mes_atual - 1 if mes_atual > 1 else 12
    ano_anterior = ano_atual if mes_atual > 1 else ano_atual - 1
    faturamento_mes_atual = data.filter((pl.col('DATA_PEDIDO').dt.month() == mes_atual) & (pl.col('DATA_PEDIDO').dt.year() == ano_atual))['VLTOTAL'].sum()
    pedidos_mes_atual = data.filter((pl.col('DATA_PEDIDO').dt.month() == mes_atual) & (pl.col('DATA_PEDIDO').dt.year() == ano_atual))['NUMPED'].n_unique()
    faturamento_mes_anterior = data.filter((pl.col('DATA_PEDIDO').dt.month() == mes_anterior) & (pl.col('DATA_PEDIDO').dt.year() == ano_anterior))['VLTOTAL'].sum()
    pedidos_mes_anterior = data.filter((pl.col('DATA_PEDIDO').dt.month() == mes_anterior) & (pl.col('DATA_PEDIDO').dt.year() == ano_anterior))['NUMPED'].n_unique()
    return faturamento_mes_atual, faturamento_mes_anterior, pedidos_mes_atual, pedidos_mes_anterior

def calcular_variacao(atual, anterior):
    if anterior == 0:
        return 0
    return ((atual - anterior) / anterior) * 100

def icone_variacao(valor):
    if valor > 0:
        return f"<span style='color: green;'>▲ {valor:.2f}%</span>"
    elif valor < 0:
        return f"<span style='color: red;'>▼ {valor:.2f}%</span>"
    else:
        return f"{valor:.2f}%"

def formatar_valor(valor):
    try:
        return locale.currency(valor, grouping=True, symbol=True)
    except Exception:
        return f"R$ {valor:,.2f}"

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

    # Inicializar session state para last_fetched
    if 'last_fetched' not in st.session_state:
        st.session_state.last_fetched = None

    # Carregar dados com cache
    with st.spinner("Carregando dados do Supabase..."):
        data = carregar_dados(_last_fetched=st.session_state.last_fetched)
        if not data.is_empty():
            st.session_state.last_fetched = data['DATA_PEDIDO'].max()

    if data.is_empty():
        st.error("Não foi possível carregar os dados. Verifique as configurações da API ou tente novamente.")
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

    if not filiais_selecionadas:
        st.warning("Por favor, selecione pelo menos uma filial para exibir os dados.")
        return

    # Filtrar dados com base nas filiais selecionadas
    data_filtrada = data.filter(pl.col('CODFILIAL').is_in(filiais_selecionadas))

    today = datetime.today()
    hoje = pd.to_datetime(today).normalize()
    ontem = hoje - timedelta(days=1)
    semana_inicial = hoje - timedelta(days=hoje.weekday())
    semana_passada_inicial = semana_inicial - timedelta(days=7)

    faturamento_hoje, faturamento_ontem, faturamento_semanal_atual, faturamento_semanal_passada = calcular_faturamento(data_filtrada, hoje, ontem, semana_inicial, semana_passada_inicial)
    pedidos_hoje, pedidos_ontem, pedidos_semanal_atual, pedidos_semanal_passada = calcular_quantidade_pedidos(data_filtrada, hoje, ontem, semana_inicial, semana_passada_inicial)

    mes_atual = hoje.month
    ano_atual = hoje.year
    faturamento_mes_atual, faturamento_mes_anterior, pedidos_mes_atual, pedidos_mes_anterior = calcular_comparativos(data_filtrada, hoje, mes_atual, ano_atual)

    # Calcular variações
    var_faturamento_mes = calcular_variacao(faturamento_mes_atual, faturamento_mes_anterior)
    var_pedidos_mes = calcular_variacao(pedidos_mes_atual, pedidos_mes_anterior)
    var_faturamento_hoje = calcular_variacao(faturamento_hoje, faturamento_ontem)
    var_pedidos_hoje = calcular_variacao(pedidos_hoje, pedidos_ontem)
    var_faturamento_semananterior = calcular_variacao(faturamento_semanal_atual, faturamento_semanal_passada)

    # Definir colunas para exibição
    col1, col2, col3, col4, col5 = st.columns(5)

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

    col_data1, col_data2 = st.columns(2)
    default_date = pd.to_datetime('2024-01-01')
    with col_data1:
        min_date = pd.to_datetime(data_filtrada['DATA_PEDIDO'].min()) if not data_filtrada.is_empty() else default_date
        max_date = pd.to_datetime(data_filtrada['DATA_PEDIDO'].max()) if not data_filtrada.is_empty() else hoje
        data_inicial = st.date_input(
            "Data Inicial",
            value=min_date.date() if min_date else default_date.date(),
            min_value=min_date.date() if min_date else None,
            max_value=max_date.date() if max_date else None
        )
    with col_data2:
        data_final = st.date_input(
            "Data Final",
            value=max_date.date() if max_date else hoje.date(),
            min_value=min_date.date() if min_date else None,
            max_value=max_date.date() if max_date else None
        )

    if data_inicial > data_final:
        st.error("A Data Inicial não pode ser maior que a Data Final.")
        return

    df_periodo = data_filtrada.filter(
        (pl.col('DATA_PEDIDO') >= pd.to_datetime(data_inicial)) &
        (pl.col('DATA_PEDIDO') <= pd.to_datetime(data_final))
    )

    if df_periodo.is_empty():
        st.warning("Nenhum dado disponível para o período selecionado.")
        return

    df_periodo = df_periodo.with_columns([
        pl.col('DATA_PEDIDO').dt.year().cast(pl.Utf8).alias('Ano'),
        pl.col('DATA_PEDIDO').dt.month().alias('Mês')
    ])

    vendas_por_mes_ano = df_periodo.group_by(['Ano', 'Mês']).agg(
        Valor_Total_Vendido=pl.col('VLTOTAL').sum()
    ).sort(['Ano', 'Mês'])

    vendas_por_mes_ano_pandas = vendas_por_mes_ano.to_pandas()

    fig = px.line(
        vendas_por_mes_ano_pandas,
        x='Mês',
        y='Valor_Total_Vendido',
        color='Ano',
        title=f'Vendas por Mês ({data_inicial} a {data_final})',
        labels={'Mês': 'Mês', 'Valor_Total_Vendido': 'Valor Total Vendido (R$)', 'Ano': 'Ano'},
        markers=True
    )

    fig.update_layout(
        title_font_size=20,
        xaxis_title_font_size=16,
        yaxis_title_font_size=16,
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14,
        xaxis_tickangle=-45,
        xaxis=dict(
            tickmode='array',
            tickvals=list(range(1, 13)),
            ticktext=['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
        )
    )

    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
