import streamlit as st
import polars as pl
import pandas as pd
from supabase import create_client, Client
import datetime
from cachetools import TTLCache
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import asyncio
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential
import nest_asyncio
import logging

# Aplicar nest_asyncio para permitir loops aninhados no Streamlit
nest_asyncio.apply()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração dos caches (TTL de 180 segundos)
cache_vendas = TTLCache(maxsize=1, ttl=180)
cache_estoque = TTLCache(maxsize=1, ttl=180)

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
        "columns": ["CODPROD", "QT", "DESCRICAO_1", "DESCRICAO_2", "DATA", "PVENDA", "VLCUSTC"],
        "date_column": "DATA"
    },
    "estoque": {
        "table": "ESTOQUE",
        "columns": ["CODFILIAL", "CODPROD", "QT_ESTOQUE", "QTULTENT", "DTULTENT", "DTULTSAIDA", "QTRESERV", 
                    "QTINDENIZ", "DTULTPEDCOMPRA", "BLOQUEADA", "NOME_PRODUTO"],
        "date_column": "DTULTENT"
    }
}

# Função para verificar se há dados na tabela (sem filtros)
async def check_data_existence(table):
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Range": "0-9"  # Buscar apenas os primeiros 10 registros
        }
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=*"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status != 200:
                    content = await response.text()
                    logger.error(f"Erro ao verificar dados em {table}: {content}")
                    return False, content
                data = await response.json()
                logger.info(f"Verificação de dados em {table}: {len(data)} registros encontrados")
                return len(data) > 0, data
    except Exception as e:
        logger.error(f"Erro ao verificar dados em {table}: {str(e)}")
        return False, str(e)

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
        logger.info(f"Executando query para {table}: {url}")
        
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
async def fetch_all_pages(table, limit=10000, max_pages=100, filter_query=None):
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
@st.cache_data(show_spinner=False, ttl=180)
def fetch_supabase_data(_cache, table, columns_expected, date_column=None, data_inicial=None, data_final=None, cache_version="v2"):
    key = f"{table}_{data_inicial}_{data_final}_{cache_version}"  # Adiciona versão para invalidar cache antigo
    if key in _cache:
        logger.info(f"Dados da tabela {table} recuperados do cache")
        return _cache[key]

    try:
        # Construir filtro de data
        filter_query = None
        if date_column and data_inicial and data_final:
            data_inicial_str = data_inicial.strftime('%Y-%m-%d')
            data_final_str = data_final.strftime('%Y-%m-%d')
            filter_query = f"{date_column}=gte.{data_inicial_str}&{date_column}=lte.{data_final_str}"

        # Executar busca assíncrona
        all_data = asyncio.run(fetch_all_pages(table, limit=10000, max_pages=100, filter_query=filter_query))

        if all_data:
            # Converter para Polars DataFrame
            df = pl.DataFrame(all_data)
            
            # Log das colunas retornadas para debug
            logger.info(f"Colunas retornadas da tabela {table}: {df.columns}")
            
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
def fetch_vendas_data(data_inicial, data_final):
    config = SUPABASE_CONFIG["vendas"]
    df = fetch_supabase_data(
        cache_vendas, 
        config["table"], 
        config["columns"], 
        date_column=config["date_column"], 
        data_inicial=data_inicial,
        data_final=data_final
    )
    if not df.is_empty():
        df = df.with_columns(pl.col('QT').cast(pl.Float64, strict=False).fill_null(0))
    return df.to_pandas()  # Converter para Pandas para compatibilidade com o código original

# Função para buscar dados de estoque (ESTOQUE)
def fetch_estoque_data(data_inicial, data_final):
    config = SUPABASE_CONFIG["estoque"]
    df = fetch_supabase_data(
        cache_estoque, 
        config["table"], 
        config["columns"], 
        date_column=config["date_column"], 
        data_inicial=data_inicial,
        data_final=data_final
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
    return df.to_pandas()  # Converter para Pandas para compatibilidade com o código original

# Função principal
def main():
    st.title("📦 Análise de Estoque e Vendas")
    st.markdown("Análise dos produtos vendidos e estoque disponível.")

    # Verificar existência de dados na tabela VWSOMELIER
    with st.spinner("Verificando existência de dados em VWSOMELIER..."):
        has_vendas, vendas_info = asyncio.run(check_data_existence("VWSOMELIER"))
    
    if not has_vendas:
        st.error(f"A tabela VWSOMELIER não contém dados ou está inacessível. Detalhes: {vendas_info}")
        return
    else:
        st.info(f"A tabela VWSOMELIER contém dados. Primeiros registros: {vendas_info[:2]}")

    # Seletor de datas
    st.markdown("### Filtro de Período")
    data_final = st.date_input("Data Final", value=datetime.date.today(), max_value=datetime.date.today())
    data_inicial = st.date_input("Data Inicial", value=data_final - datetime.timedelta(days=60))

    if data_inicial > data_final:
        st.error("A Data Inicial não pode ser maior que a Data Final.")
        return

    logger.info(f"Buscando dados de {data_inicial} até {data_final}")

    # Buscar dados de vendas (VW_SOMELIER)
    with st.spinner("Carregando dados de vendas..."):
        vendas_df = fetch_vendas_data(data_inicial, data_final)

    if vendas_df.empty:
        st.warning("Não há vendas para o período selecionado.")
        return

    # Agrupar as vendas por produto e somar as quantidades vendidas
    vendas_grouped = vendas_df.groupby('CODPROD')['QT'].sum().reset_index()

    # Buscar dados de estoque (PCEST)
    with st.spinner("Carregando dados de estoque..."):
        estoque_df = fetch_estoque_data(data_inicial, data_final)

    if estoque_df.empty:
        st.warning("Não há dados de estoque para o período selecionado.")
        return

    # Verificar se os produtos com alta venda estão sem estoque
    # Incluir CODFILIAL no merge
    merged_df = pd.merge(vendas_grouped, estoque_df[['CODPROD', 'QT_ESTOQUE', 'NOME_PRODUTO', 'CODFILIAL']], on='CODPROD', how='left')

    # Filtrando os produtos que NÃO possuem estoque
    sem_estoque_df = merged_df[merged_df['QT_ESTOQUE'].isna() | (merged_df['QT_ESTOQUE'] <= 0)]

    # Barra de pesquisa para código do produto
    pesquisar = st.text_input("Pesquisar por Código do Produto", "")
    
    # Buscar dados do endpoint PCEST com os parâmetros de data
    df = fetch_estoque_data(data_inicial, data_final)

    if not df.empty:
        # Renomear as colunas
        df = df.rename(columns={
            'CODPROD': 'Código do Produto',
            'NOME_PRODUTO': 'Nome do Produto',
            'QTULTENT': 'Quantidade Última Entrada',
            'QT_ESTOQUE': 'Estoque Disponível',
            'QTRESERV': 'Quantidade Reservada',
            'QTINDENIZ': 'Quantidade Avariada',
            'DTULTENT': 'Data Última Entrada',
            'DTULTSAIDA': 'Data Última Saída',
            'CODFILIAL': 'Código da Filial',
            'DTULTPEDCOMPRA': 'Data Último Pedido Compra',
            'BLOQUEADA': 'Quantidade Bloqueada'
        })

        if pesquisar:
            if pesquisar.isdigit():
                df = df[df['Código do Produto'].astype(str) == pesquisar]
            else:
                df = df[df['Nome do Produto'].str.contains(pesquisar, case=False, na=False)]

        df['Quantidade Total'] = df[['Estoque Disponível', 'Quantidade Reservada', 'Quantidade Bloqueada']].fillna(0).sum(axis=1)

        # Reordenar as colunas
        df = df.reindex(columns=[
            'Código da Filial', 'Código do Produto', 'Nome do Produto', 'Estoque Disponível', 'Quantidade Reservada', 
            'Quantidade Bloqueada', 'Quantidade Avariada', 'Quantidade Total', 'Quantidade Última Entrada', 
            'Data Última Entrada', 'Data Última Saída', 'Data Último Pedido Compra'
        ])

        # Configurar a tabela de estoque com AgGrid e rolagem vertical
        st.subheader("✅ Estoque")
        st.markdown("Use a barra de rolagem para ver mais linhas.")
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
        gb.configure_pagination(enabled=False)  # Desativar paginação para permitir rolagem
        gb.configure_grid_options(domLayout='normal')
        grid_options = gb.build()

        # Formatar números para exibição
        df_display = df.copy()
        df_display['Estoque Disponível'] = df_display['Estoque Disponível'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        df_display['Quantidade Reservada'] = df_display['Quantidade Reservada'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        df_display['Quantidade Bloqueada'] = df_display['Quantidade Bloqueada'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        df_display['Quantidade Avariada'] = df_display['Quantidade Avariada'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        df_display['Quantidade Total'] = df_display['Quantidade Total'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        df_display['Quantidade Última Entrada'] = df_display['Quantidade Última Entrada'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")

        AgGrid(
            df_display,
            gridOptions=grid_options,
            update_mode=GridUpdateMode.NO_UPDATE,
            allow_unsafe_jscode=True,
            height=400,  # Altura fixa para ativar rolagem vertical
            theme='streamlit'
        )

    if sem_estoque_df.empty:
        st.info("Não há produtos vendidos sem estoque.")
    else:
        # Exibir a tabela com os produtos sem estoque mas vendidos
        st.subheader("❌ Produtos Sem Estoque com Venda nos Últimos 2 Meses")

        # Excluir produtos com estoque > 0
        sem_estoque_df = sem_estoque_df[sem_estoque_df['QT_ESTOQUE'].isna() | (sem_estoque_df['QT_ESTOQUE'] <= 0)]

        # Renomear as colunas, incluindo CODFILIAL
        sem_estoque_df = sem_estoque_df.rename(columns={
            'NOME_PRODUTO': 'Nome do Produto',
            'CODPROD': 'Código Produto',
            'QT': 'Quantidade Vendida',
            'QT_ESTOQUE': 'Estoque Disponível',
            'CODFILIAL': 'Código da Filial'
        })

        # Filtrar para remover linhas onde 'Nome do Produto' é NaN ou vazio
        sem_estoque_df = sem_estoque_df[
            sem_estoque_df['Nome do Produto'].notna() & 
            (sem_estoque_df['Nome do Produto'] != '')
        ]
        # Converter todas as colunas para maiúsculas
        sem_estoque_df.columns = [col.upper() for col in sem_estoque_df.columns]

        # Configurar a tabela de produtos sem estoque com AgGrid e rolagem vertical
        gb = GridOptionsBuilder.from_dataframe(sem_estoque_df)
        gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
        gb.configure_column("CÓDIGO PRODUTO", width=150)
        gb.configure_column("NOME DO PRODUTO", width=300)
        gb.configure_column("QUANTIDADE VENDIDA", width=150)
        gb.configure_column("ESTOQUE DISPONÍVEL", width=150)
        gb.configure_column("CÓDIGO DA FILIAL", width=120)
        gb.configure_pagination(enabled=False)  # Desativar paginação para permitir rolagem
        gb.configure_grid_options(domLayout='normal', autoSizeColumns=False)  # Usar larguras fixas
        grid_options = gb.build()

        # Formatar números para exibição
        df_sem_estoque_display = sem_estoque_df.copy()
        df_sem_estoque_display['QUANTIDADE VENDIDA'] = df_sem_estoque_display['QUANTIDADE VENDIDA'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        df_sem_estoque_display['ESTOQUE DISPONÍVEL'] = df_sem_estoque_display['ESTOQUE DISPONÍVEL'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")

        AgGrid(
            df_sem_estoque_display[['CÓDIGO PRODUTO', 'NOME DO PRODUTO', 'QUANTIDADE VENDIDA', 'ESTOQUE DISPONÍVEL', 'CÓDIGO DA FILIAL']],
            gridOptions=grid_options,
            update_mode=GridUpdateMode.NO_UPDATE,
            allow_unsafe_jscode=True,
            height=300,  # Altura fixa para ativar rolagem vertical
            theme='streamlit',
            fit_columns_on_grid_load=False  # Usar larguras fixas definidas
        )

if __name__ == "__main__":
    main()
