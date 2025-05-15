import streamlit as st
import polars as pl
import pandas as pd
from supabase import create_client, Client
import datetime
from cachetools import TTLCache
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import time
import logging
import asyncio
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential
import nest_asyncio

# Aplicar nest_asyncio para permitir loops aninhados no Streamlit
nest_asyncio.apply()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração dos caches (TTL de 300 segundos)
cache_vendas = TTLCache(maxsize=1, ttl=300)
cache_estoque = TTLCache(maxsize=1, ttl=300)

# Configuração do Supabase usando secrets do Streamlit Cloud
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

# Configuração das tabelas e colunas esperadas
SUPABASE_CONFIG = {
    "vendas": {
        "table": "VWSOMELIER",
        "columns": ["CODPROD", "QT", "DESCRICAO_1", "DESCRICAO_2", "DATA"],
        "date_column": "DATA",
        "filial_filter": "CODFILIAL=in.('1','2')"
    },
    "estoque": {
        "table": "ESTOQUE",
        "columns": ["CODFILIAL", "CODPROD", "QT_ESTOQUE", "QTULTENT", "DTULTENT", "DTULTSAIDA", "QTRESERV", 
                    "QTINDENIZ", "DTULTPEDCOMPRA", "BLOQUEADA", "NOME_PRODUTO"],
        "date_column": "DTULTENT",
        "filial_filter": "CODFILIAL=in.('1','2')"
    }
}

# Função para buscar dados do Supabase com paginação e retry (assíncrona)
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
async def fetch_supabase_page_async(session, table, offset, limit, filter_query=None):
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Range": f"{offset}-{offset + limit - 1}"
        }
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=*"
        if filter_query:
            url += f"&{filter_query}"
        
        async with session.get(url, headers=headers, timeout=30) as response:
            if response.status != 200:
                content = await response.text()
                logger.error(f"Erro HTTP {response.status} ao buscar {table}: {content}")
                raise Exception(f"HTTP {response.status}: {content}")
            data = await response.json()
            logger.info(f"Recuperados {len(data)} registros da tabela {table}, offset {offset}")
            return data
    except Exception as e:
        logger.error(f"Erro ao buscar página da tabela {table}, offset {offset}: {str(e)}")
        raise

# Função para buscar todas as páginas de uma tabela assincronamente
async def fetch_all_pages(table, limit=10000, max_pages=5000, filter_query=None):
    all_data = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for offset in range(0, limit * max_pages, limit):
            tasks.append(fetch_supabase_page_async(session, table, offset, limit, filter_query))
        
        for future in asyncio.as_completed(tasks):
            try:
                data = await future
                if data:
                    all_data.extend(data)
                else:
                    break
            except Exception as e:
                logger.error(f"Erro em uma requisição para {table}: {str(e)}")
                continue
    
    return all_data

# Função para buscar dados do Supabase com cache e Polars
@st.cache_data(show_spinner=False, ttl=300)
def fetch_supabase_data(_cache, table, columns_expected, date_column=None, filial_filter=None, last_update=None):
    key = f"{table}_{last_update or 'full'}"
    if key in _cache:
        logger.info(f"Dados da tabela {table} recuperados do cache")
        return _cache[key]

    try:
        # Construir filtro de data incremental, se aplicável
        filter_query = filial_filter
        if last_update and date_column:
            last_update_str = last_update.strftime('%Y-%m-%d')
            filter_query = f"{filial_filter}&{date_column}=gte.{last_update_str}" if filial_filter else f"{date_column}=gte.{last_update_str}"

        # Executar busca assíncrona
        all_data = asyncio.run(fetch_all_pages(table, limit=10000, max_pages=5000, filter_query=filter_query))

        if all_data:
            # Converter para Polars DataFrame
            df = pl.DataFrame(all_data)
            
            # Verificar colunas esperadas
            missing_columns = [col for col in columns_expected if col not in df.columns]
            if missing_columns:
                logger.error(f"Colunas ausentes na tabela {table}: {missing_columns}")
                st.error(f"Colunas ausentes na tabela {table}: {missing_columns}")
                _cache[key] = pl.DataFrame()
                return pl.DataFrame()

            # Garantir tipos de dados
            if date_column and date_column in df.columns:
                df = df.with_columns(pl.col(date_column).str.to_datetime(format="%Y-%m-%d", strict=False))
                df = df.filter(pl.col(date_column).is_not_null())

            _cache[key] = df
            logger.info(f"Dados carregados com sucesso da tabela {table}: {len(df)} registros")
        else:
            logger.warning(f"Nenhum dado retornado da tabela {table}")
            st.warning(f"Nenhum dado retornado da tabela {table}.")
            _cache[key] = pl.DataFrame()
            df = pl.DataFrame()

    except Exception as e:
        logger.error(f"Erro ao buscar dados da tabela {table}: {str(e)}")
        st.error(f"Erro ao buscar dados da tabela {table}: {str(e)}")
        _cache[key] = pl.DataFrame()
        df = pl.DataFrame()

    return _cache[key]

# Função para buscar dados de vendas (VWSOMELIER)
def fetch_vendas_data():
    config = SUPABASE_CONFIG["vendas"]
    last_update = st.session_state.get('last_vendas_update', None)
    df = fetch_supabase_data(
        cache_vendas, 
        config["table"], 
        config["columns"], 
        date_column=config["date_column"], 
        filial_filter=config["filial_filter"], 
        last_update=last_update
    )
    if not df.is_empty():
        df = df.with_columns(pl.col('QT').cast(pl.Float64, strict=False).fill_null(0))
        if config["date_column"] in df.columns:
            max_date = df[config["date_column"]].max()
            if max_date is not None:
                st.session_state['last_vendas_update'] = max_date
    return df

# Função para buscar dados de estoque (ESTOQUE)
def fetch_estoque_data():
    config = SUPABASE_CONFIG["estoque"]
    last_update = st.session_state.get('last_estoque_update', None)
    df = fetch_supabase_data(
        cache_estoque, 
        config["table"], 
        config["columns"], 
        date_column=config["date_column"], 
        filial_filter=config["filial_filter"], 
        last_update=last_update
    )
    if not df.is_empty():
        df = df.with_columns([
            pl.col(col).cast(pl.Float64, strict=False).fill_null(0)
            for col in ['QTULTENT', 'QT_ESTOQUE', 'QTRESERV', 'QTINDENIZ', 'BLOQUEADA']
            if col in df.columns
        ]).with_columns([
            pl.col(col).str.to_datetime(format="%Y-%m-%d", strict=False)
            for col in ['DTULTENT', 'DTULTSAIDA', 'DTULTPEDCOMPRA']
            if col in df.columns
        ])
        if config["date_column"] in df.columns:
            max_date = df[config["date_column"]].max()
            if max_date is not None:
                st.session_state['last_estoque_update'] = max_date
    return df

# Função para realizar o reload automático a cada 120 segundos
def auto_reload():
    if 'last_reload' not in st.session_state:
        st.session_state.last_reload = time.time()
    
    current_time = time.time()
    if current_time - st.session_state.last_reload >= 120:
        st.session_state.last_reload = current_time
        st.cache_data.clear()
        st.rerun()

# Função principal
def main():
    st.title("📦 Análise de Estoque e Vendas")
    st.markdown("Análise dos produtos vendidos e estoque disponível.")

    # Chamar auto_reload para verificar se precisa atualizar
    auto_reload()

    # Definir as datas de início e fim para os últimos 2 meses
    data_final = datetime.date.today()  # 15/05/2025
    data_inicial = data_final - datetime.timedelta(days=60)  # 16/03/2025

    # Buscar dados de vendas (VWSOMELIER)
    with st.spinner("Carregando dados de vendas..."):
        vendas_df = fetch_vendas_data()

    if vendas_df.is_empty():
        st.warning("Não há vendas para o período selecionado.")
        vendas_grouped = pl.DataFrame()
    else:
        # Agrupar as vendas por produto e somar as quantidades vendidas
        vendas_grouped = vendas_df.group_by('CODPROD').agg(QT=pl.col('QT').sum())

    # Buscar dados de estoque (ESTOQUE)
    with st.spinner("Carregando dados de estoque..."):
        estoque_df = fetch_estoque_data()

    if estoque_df.is_empty():
        st.warning("Não há dados de estoque para o período selecionado.")
        merged_df = pl.DataFrame()
    else:
        # Verificar se os produtos com alta venda estão sem estoque
        merged_df = vendas_grouped.join(
            estoque_df.select(['CODPROD', 'NOME_PRODUTO', 'QT_ESTOQUE']),
            on='CODPROD',
            how='left'
        )

        # Filtrando os produtos que NÃO possuem estoque
        sem_estoque_df = merged_df.filter(
            pl.col('QT_ESTOQUE').is_null() | (pl.col('QT_ESTOQUE') <= 0)
        )

        # Barra de pesquisa para código do produto
        pesquisar = st.text_input("Pesquisar por Código do Produto ou Nome", "")

        # Renomear as colunas
        df = estoque_df.rename({
            'CODFILIAL': 'Código da Filial',
            'CODPROD': 'Código do Produto',
            'NOME_PRODUTO': 'Nome do Produto',
            'QTULTENT': 'Quantidade Última Entrada',
            'QT_ESTOQUE': 'Estoque Disponível',
            'QTRESERV': 'Quantidade Reservada',
            'QTINDENIZ': 'Quantidade Avariada',
            'DTULTENT': 'Data Última Entrada',
            'DTULTSAIDA': 'Data Última Saída',
            'DTULTPEDCOMPRA': 'Data Último Pedido Compra',
            'BLOQUEADA': 'Quantidade Bloqueada'
        })

        if pesquisar:
            df = df.filter(
                pl.col('Código do Produto').cast(pl.Utf8).str.contains(pesquisar, case=False) |
                pl.col('Nome do Produto').str.contains(pesquisar, case=False)
            )

        df = df.with_columns(
            (pl.col('Estoque Disponível').fill_null(0) + 
             pl.col('Quantidade Reservada').fill_null(0) + 
             pl.col('Quantidade Bloqueada').fill_null(0)).alias('Quantidade Total')
        )

        # Reordenar as colunas
        df = df.select([
            'Código da Filial', 'Código do Produto', 'Nome do Produto', 'Estoque Disponível', 'Quantidade Reservada',
            'Quantidade Bloqueada', 'Quantidade Avariada', 'Quantidade Total', 'Quantidade Última Entrada',
            'Data Última Entrada', 'Data Última Saída', 'Data Último Pedido Compra'
        ])

        # Formatar números e datas em Polars antes de converter para Pandas
        df = df.with_columns([
            pl.col(col).cast(pl.Int64, strict=False).fill_null(0).map_elements(lambda x: f"{x:,}", return_dtype=pl.Utf8).alias(col)
            for col in ['Estoque Disponível', 'Quantidade Reservada', 'Quantidade Bloqueada', 'Quantidade Avariada', 'Quantidade Total', 'Quantidade Última Entrada']
        ]).with_columns([
            pl.col(col).cast(pl.Date, strict=False).map_elements(lambda x: x.strftime('%Y-%m-%d') if x is not None else "", return_dtype=pl.Utf8).alias(col)
            for col in ['Data Última Entrada', 'Data Última Saída', 'Data Último Pedido Compra']
        ])

        # Converter para Pandas para AgGrid
        df_pandas = df.to_pandas()

        # Configurar a tabela de estoque com AgGrid e larguras fixas
        st.subheader("✅ Estoque")
        st.markdown("Use a barra de rolagem para ver mais linhas.")
        gb = GridOptionsBuilder.from_dataframe(df_pandas)
        gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=False)
        gb.configure_column("Código da Filial", width=100)
        gb.configure_column("Código do Produto", width=120)
        gb.configure_column("Nome do Produto", width=250)
        gb.configure_column("Estoque Disponível", width=120)
        gb.configure_column("Quantidade Reservada", width=120)
        gb.configure_column("Quantidade Bloqueada", width=120)
        gb.configure_column("Quantidade Avariada", width=120)
        gb.configure_column("Quantidade Total", width=120)
        gb.configure_column("Quantidade Última Entrada", width=120)
        gb.configure_column("Data Última Entrada", width=130)
        gb.configure_column("Data Última Saída", width=130)
        gb.configure_column("Data Último Pedido Compra", width=130)
        gb.configure_pagination(enabled=False)
        gb.configure_grid_options(domLayout='normal')
        grid_options = gb.build()

        AgGrid(
            df_pandas,
            gridOptions=grid_options,
            update_mode=GridUpdateMode.NO_UPDATE,
            allow_unsafe_jscode=True,
            height=400,
            theme='streamlit',
            fit_columns_on_grid_load=False
        )

        if sem_estoque_df.is_empty():
            st.info("Não há produtos vendidos sem estoque.")
        else:
            # Exibir a tabela com os produtos sem estoque mas vendidos
            st.subheader("❌ Produtos Sem Estoque com Venda nos Últimos 2 Meses")

            sem_estoque_df = sem_estoque_df.rename({
                'CODPROD': 'CÓDIGO PRODUTO',
                'NOME_PRODUTO': 'NOME DO PRODUTO',
                'QT': 'QUANTIDADE VENDIDA',
                'QT_ESTOQUE': 'ESTOQUE TOTAL'
            })

            sem_estoque_df = sem_estoque_df.filter(
                pl.col('NOME DO PRODUTO').is_not_null() & (pl.col('NOME DO PRODUTO') != '')
            )

            sem_estoque_df = sem_estoque_df.select([
                'CÓDIGO PRODUTO', 'NOME DO PRODUTO', 'QUANTIDADE VENDIDA', 'ESTOQUE TOTAL'
            ])

            # Formatar números em Polars
            sem_estoque_df = sem_estoque_df.with_columns([
                pl.col('QUANTIDADE VENDIDA').cast(pl.Int64, strict=False).fill_null(0).map_elements(lambda x: f"{x:,}", return_dtype=pl.Utf8),
                pl.col('ESTOQUE TOTAL').cast(pl.Int64, strict=False).fill_null(0).map_elements(lambda x: f"{x:,}", return_dtype=pl.Utf8)
            ])

            # Converter para Pandas para AgGrid
            sem_estoque_df_pandas = sem_estoque_df.to_pandas()

            gb = GridOptionsBuilder.from_dataframe(sem_estoque_df_pandas)
            gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=False)
            gb.configure_column("CÓDIGO PRODUTO", width=150)
            gb.configure_column("NOME DO PRODUTO", width=300)
            gb.configure_column("QUANTIDADE VENDIDA", width=200)
            gb.configure_column("ESTOQUE TOTAL", width=200)
            gb.configure_pagination(enabled=False)
            gb.configure_grid_options(domLayout='normal')
            grid_options = gb.build()

            AgGrid(
                sem_estoque_df_pandas,
                gridOptions=grid_options,
                update_mode=GridUpdateMode.NO_UPDATE,
                allow_unsafe_jscode=True,
                height=300,
                theme='streamlit',
                fit_columns_on_grid_load=True
            )

if __name__ == "__main__":
    main()
