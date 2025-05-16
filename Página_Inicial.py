import streamlit as st
import polars as pl
import pandas as pd
import requests
import sqlite3
from datetime import datetime, timedelta, date
import locale
import plotly.express as px
import logging
from concurrent.futures import ThreadPoolExecutor
import json
import os
from urllib.parse import urlencode
import threading
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Definir o local para a formatação monetária
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    logger.warning("Locale 'pt_BR.UTF-8' não disponível, usando padrão.")
    locale.setlocale(locale.LC_ALL, '')

# Configuração da conexão com o Supabase
class SupabaseConnection:
    def __init__(self, url, key, db_path="supabase_cache.db"):
        self.base_url = url
        self.key = key
        self.db_path = db_path
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Accept": "application/json"
        }
        self.tables = [
            {
                "table_name": "PCPEDC",
                "url": f"{self.base_url}/rest/v1/PCPEDC?select=*"
            }
        ]
        self._init_cache()
        self._memory_cache = None  # In-memory cache
        self._lock = threading.Lock()  # Thread-safe cache updates
        self._last_sync = 0  # Timestamp of last sync
        self._start_background_sync()

    def _init_cache(self):
        """Initialize SQLite database for caching with indexes."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS PCPEDC (
                        NUMPED TEXT PRIMARY KEY,
                        PVENDA REAL,
                        QT INTEGER,
                        CODFILIAL TEXT,
                        DATA_PEDIDO TEXT,
                        VLTOTAL REAL,
                        last_updated TEXT
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                """)
                # Add indexes for faster queries
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_pedido ON PCPEDC(DATA_PEDIDO)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_codfilial ON PCPEDC(CODFILIAL)")
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Erro ao inicializar cache SQLite: {e}")
            st.error(f"Erro ao inicializar cache: {e}")
            raise

    def _get_latest_timestamp(self):
        """Get the latest DATA_PEDIDO from the cache, or a default for initial fetch."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(DATA_PEDIDO) FROM PCPEDC")
                result = cursor.fetchone()[0]
                if result:
                    # Use a 7-day window to catch delayed updates
                    latest_date = datetime.fromisoformat(result.replace('Z', '+00:00'))
                    return (latest_date - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
                return "1970-01-01T00:00:00Z"
        except sqlite3.Error as e:
            logger.error(f"Erro ao obter último timestamp: {e}")
            return "1970-01-01T00:00:00Z"

    def fetch_new_data(self, table, page_size=1000):
        """Fetch data from Supabase with retries, using record count for pagination."""
        table_name = table["table_name"]
        latest_timestamp = self._get_latest_timestamp()
        logger.info(f"Buscando dados da tabela {table_name} após {latest_timestamp}")

        # Construir URL com filtro para novos dados (skip filter for initial fetch)
        url = table["url"]
        if latest_timestamp != "1970-01-01T00:00:00Z":
            query_params = {
                "DATA_PEDIDO": f"gte.{latest_timestamp}",
                "order": "DATA_PEDIDO.asc"
            }
            url = f"{url}&{urlencode(query_params)}"
        
        all_data = []
        offset = 0
        # Configurar sessão com retries
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        while True:
            headers = self.headers.copy()
            headers["Range"] = f"{offset}-{offset + page_size - 1}"
            try:
                response = session.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                if not data:
                    logger.info(f"Finalizada a recuperação de dados da tabela {table_name}")
                    break
                all_data.extend(data)
                offset += len(data)
                logger.info(f"Recuperados {len(data)} registros da tabela {table_name}, total: {len(all_data)}")
                
                # Stop if fewer than page_size records are returned
                if len(data) < page_size:
                    break
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro ao buscar dados da tabela {table_name}: {e}")
                return all_data  # Return partial data if available
            finally:
                session.close()

        # Log unique CODFILIAL values for debugging
        codfiliais = set(record.get('CODFILIAL') for record in all_data)
        logger.info(f"Valores de CODFILIAL encontrados: {codfiliais}")

        return all_data

    def update_cache(self, data):
        """Update SQLite cache with new or updated data using batch inserts."""
        if not data:
            return
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                batch = []
                for record in data:
                    pvenda = float(record.get('PVENDA', 0)) if record.get('PVENDA') else 0
                    qt = int(record.get('QT', 0)) if record.get('QT') else 0
                    vltotal = pvenda * qt
                    batch.append((
                        record.get('NUMPED'),
                        pvenda,
                        qt,
                        str(record.get('CODFILIAL', '')),
                        record.get('DATA_PEDIDO'),
                        vltotal,
                        record.get('DATA_PEDIDO')
                    ))
                cursor.executemany("""
                    INSERT OR REPLACE INTO PCPEDC (
                        NUMPED, PVENDA, QT, CODFILIAL, DATA_PEDIDO, VLTOTAL, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, batch)
                conn.commit()
                logger.info(f"Cache atualizado com {len(data)} registros")
        except sqlite3.Error as e:
            logger.error(f"Erro ao atualizar cache: {e}")
            st.error(f"Erro ao atualizar cache: {e}")

    def load_from_cache(self):
        """Load all data from SQLite cache into a Polars DataFrame."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = "SELECT NUMPED, PVENDA, QT, CODFILIAL, DATA_PEDIDO, VLTOTAL FROM PCPEDC"
                df = pl.read_database(query, conn)
                if df.is_empty():
                    logger.warning("Cache vazio")
                    return pl.DataFrame()
                # Garantir tipos de dados
                df = df.with_columns([
                    pl.col('PVENDA').cast(pl.Float32).fill_null(0),
                    pl.col('QT').cast(pl.Int32).fill_null(0),
                    pl.col('CODFILIAL').cast(pl.Utf8),
                    pl.col('NUMPED').cast(pl.Utf8),
                    pl.col('DATA_PEDIDO').str.to_datetime(format="%Y-%m-%d", strict=False),
                    pl.col('VLTOTAL').cast(pl.Float32).fill_null(0)
                ])
                logger.info(f"Carregados {len(df)} registros do cache")
                return df
        except Exception as e:
            logger.error(f"Erro ao carregar dados do cache: {e}")
            return pl.DataFrame()

    def _update_memory_cache(self):
        """Update in-memory cache with latest data."""
        with self._lock:
            data = self.load_from_cache()
            if not data.is_empty():
                self._memory_cache = data
                self._last_sync = time.time()
                logger.info(f"Memória cache atualizada: {len(data)} registros")

    def sync_data(self):
        """Synchronize cache with Supabase and update memory cache."""
        try:
            with ThreadPoolExecutor() as executor:
                new_data = list(executor.map(self.fetch_new_data, self.tables))
            all_new_data = []
            for result in new_data:
                all_new_data.extend(result)
            self.update_cache(all_new_data)
            self._update_memory_cache()
            data = self._memory_cache if self._memory_cache is not None else self.load_from_cache()
            if not data.is_empty():
                date_range = (data['DATA_PEDIDO'].min(), data['DATA_PEDIDO'].max())
                logger.info(f"Dados carregados: {len(data)} registros, intervalo de datas: {date_range}")
            return data
        except Exception as e:
            logger.error(f"Erro ao sincronizar dados: {e}")
            # Fallback to memory cache or SQLite cache
            with self._lock:
                if self._memory_cache is not None:
                    logger.info(f"Usando memória cache após falha de sincronização: {len(self._memory_cache)} registros")
                    return self._memory_cache
            data = self.load_from_cache()
            if not data.is_empty():
                logger.info(f"Usando dados do cache SQLite após falha de sincronização: {len(data)} registros")
                return data
            st.error(f"Erro ao sincronizar dados: {e}")
            return pl.DataFrame()

    def _background_sync(self):
        """Run sync_data periodically in the background."""
        SYNC_INTERVAL = 300  # 5 minutes in seconds
        while True:
            try:
                if time.time() - self._last_sync >= SYNC_INTERVAL:
                    logger.info("Iniciando sincronização em segundo plano")
                    self.sync_data()
            except Exception as e:
                logger.error(f"Erro na sincronização em segundo plano: {e}")
            time.sleep(SYNC_INTERVAL)

    def _start_background_sync(self):
        """Start background sync thread."""
        thread = threading.Thread(target=self._background_sync, daemon=True)
        thread.start()
        logger.info("Sincronização em segundo plano iniciada")

# Função para carregar dados com cache
@st.cache_data(show_spinner=False, ttl=900)
def carregar_dados():
    try:
        supabase = SupabaseConnection(
            url=st.secrets["SUPABASE_URL"],
            key=st.secrets["SUPABASE_KEY"]
        )
        # Sincronizar dados (carrega do cache e busca novos dados)
        data = supabase.sync_data()
        if data.is_empty():
            logger.warning("Nenhum dado disponível após sincronização")
            st.error("Nenhum dado disponível. Verifique a conexão com o Supabase.")
            return pl.DataFrame()

        # Filtrar apenas filiais 1 e 2
        data = data.filter(pl.col('CODFILIAL').is_in(['1', '2']))

        # Remover registros com DATA_PEDIDO nula
        if data['DATA_PEDIDO'].is_null().any():
            logger.warning("Valores inválidos encontrados na coluna 'DATA_PEDIDO'.")
            st.warning("Valores inválidos encontrados na coluna 'DATA_PEDIDO'. Filtrando registros inválidos.")
            data = data.filter(pl.col('DATA_PEDIDO').is_not_null())

        logger.info(f"Dados carregados com sucesso: {len(data)} registros")
        return data
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
    with st.spinner("Carregando dados..."):
        data = carregar_dados()
    
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

    # Verificar se pelo menos uma filial está selecionada
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

    # Seletores de data
    col_data1, col_data2 = st.columns(2)
    with col_data1:
        min_date = data_filtrada['DATA_PEDIDO'].min().date() if not data_filtrada.is_empty() else date(2024, 1, 1)
        data_inicial = st.date_input(
            "Data Inicial",
            value=min_date,
            min_value=min_date,
            max_value=date.today()
        )
    with col_data2:
        max_date = data_filtrada['DATA_PEDIDO'].max().date() if not data_filtrada.is_empty() else date.today()
        data_final = st.date_input(
            "Data Final",
            value=date.today(),
            min_value=min_date,
            max_value=date.today()
        )

    if data_inicial > data_final:
        st.error("A Data Inicial não pode ser maior que a Data Final.")
        return

    # Filtrar dados pelo período selecionado
    df_periodo = data_filtrada.filter(
        (pl.col('DATA_PEDIDO') >= pd.to_datetime(data_inicial)) & 
        (pl.col('DATA_PEDIDO') <= pd.to_datetime(data_final))
    )

    if df_periodo.is_empty():
        st.warning("Nenhum dado disponível para o período selecionado.")
        return

    # Adicionar colunas de ano e mês
    df_periodo = df_periodo.with_columns([
        pl.col('DATA_PEDIDO').dt.year().cast(pl.Utf8).alias('Ano'),
        pl.col('DATA_PEDIDO').dt.month().alias('Mês')
    ])

    # Agrupar por ano e mês
    vendas_por_mes_ano = df_periodo.group_by(['Ano', 'Mês']).agg(
        Valor_Total_Vendido=pl.col('VLTOTAL').sum()
    ).sort(['Ano', 'Mês'])

    # Converter para Pandas para compatibilidade com Plotly
    vendas_por_mes_ano_pandas = vendas_por_mes_ano.to_pandas()

    # Criar gráfico de linhas com uma linha por ano
    fig = px.line(
        vendas_por_mes_ano_pandas,
        x='Mês',
        y='Valor_Total_Vendido',
        color='Ano',
        title=f'Vendas por Mês ({data_inicial} a {data_final})',
        labels={'Mês': 'Mês', 'Valor_Total_Vendido': 'Valor Total Vendido (R$)', 'Ano': 'Ano'},
        markers=True
    )

    # Ajustes visuais
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
