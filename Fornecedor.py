import streamlit as st
import polars as pl
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from cachetools import TTLCache
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import aiohttp
import asyncio
import nest_asyncio
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
import time

# Aplicar nest_asyncio para compatibilidade com Streamlit
nest_asyncio.apply()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do cache (TTL de 300 segundos)
cache = TTLCache(maxsize=1, ttl=300)

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

# Configuração da tabela e colunas esperadas
SUPABASE_CONFIG = {
    "vendas": {
        "table": "PCVENDEDOR2",
        "columns": ["DATA", "QT", "PVENDA", "FORNECEDOR", "VENDEDOR", "CLIENTE", "PRODUTO", "CODPROD", "CODIGOVENDEDOR", "CODCLI"],
        "date_column": "DATA",
    }
}

# Função para buscar página do Supabase com retry (assíncrona)
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

# Função para buscar todas as páginas assincronamente
async def fetch_all_pages(table, limit=50000, max_pages=10000, filter_query=None):
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
def get_data_from_supabase(_cache, data_inicial, data_final):
    key = f"{data_inicial.strftime('%Y-%m-%d')}_{data_final.strftime('%Y-%m-%d')}"
    if key in _cache:
        logger.info(f"Dados recuperados do cache para {key}")
        return _cache[key]

    config = SUPABASE_CONFIG["vendas"]
    table = config["table"]
    columns_expected = config["columns"]
    date_column = config["date_column"]

    try:
        # Construir filtro de data
        data_inicial_str = data_inicial.strftime('%Y-%m-%d')
        data_final_str = data_final.strftime('%Y-%m-%d')
        filter_query = (
            f"{date_column}=gte.{data_inicial_str}&{date_column}=lte.{data_final_str}"
        )
        logger.info(f"Filter query: {filter_query}")

        # Debug: Fetch a small sample without filters to check table contents
        sample_data = asyncio.run(fetch_all_pages(table, limit=10, max_pages=1, filter_query=None))
        if sample_data:
            logger.info(f"Sample data (first 10 rows): {sample_data}")
            st.write("Sample data from PCVENDEDOR2 (first 10 rows):")
            st.json(sample_data)
        else:
            logger.warning("No data in PCVENDEDOR2 table (sample query).")
            st.warning("No data found in PCVENDEDOR2 table (sample query).")

        # Executar busca assíncrona com filtros
        all_data = asyncio.run(fetch_all_pages(table, limit=50000, max_pages=10000, filter_query=filter_query))

        if all_data:
            df = pl.DataFrame(all_data)
            
            # Verificar colunas esperadas
            missing_columns = [col for col in columns_expected if col not in df.columns]
            if missing_columns:
                logger.error(f"Colunas ausentes na tabela {table}: {missing_columns}")
                st.error(f"Colunas ausentes na tabela {table}: {missing_columns}")
                _cache[key] = pl.DataFrame()
                return pl.DataFrame()

            # Garantir tipos de dados
            df = df.with_columns([
                pl.col('DATA').str.to_datetime(format="%Y-%m-%d", strict=False),
                pl.col('QT').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('PVENDA').cast(pl.Float64, strict=False).fill_null(0)
            ]).filter(pl.col('DATA').is_not_null())

            # Validar dados
            if df['QT'].lt(0).any():
                logger.warning("Quantidades negativas encontradas em 'QT'. Substituindo por 0.")
                df = df.with_columns(pl.col('QT').clip(min=0))
            if df['PVENDA'].lt(0).any():
                logger.warning("Preços negativos encontrados em 'PVENDA'. Substituindo por 0.")
                df = df.with_columns(pl.col('PVENDA').clip(min=0))

            # Calcular valor total e extrair mês/ano
            df = df.with_columns([
                (pl.col('QT') * pl.col('PVENDA')).alias('VALOR_TOTAL_ITEM'),
                pl.col('DATA').dt.month().alias('MES'),
                pl.col('DATA').dt.year().alias('ANO')
            ])

            _cache[key] = df
            logger.info(f"Dados carregados com sucesso: {len(df)} registros")
        else:
            logger.warning(f"Nenhum dado retornado da tabela {table} para o filtro: {filter_query}")
            st.warning(f"Nenhum dado retornado da tabela {table} para o filtro: {filter_query}")
            _cache[key] = pl.DataFrame()
            df = pl.DataFrame()

    except Exception as e:
        logger.error(f"Erro ao buscar dados da tabela {table}: {str(e)}")
        st.error(f"Erro ao buscar dados: {str(e)}")
        _cache[key] = pl.DataFrame()
        df = pl.DataFrame()

    return _cache[key]

# Função para realizar o reload automático a cada 30 segundos
def auto_reload():
    if 'last_reload' not in st.session_state:
        st.session_state.last_reload = time.time()
    
    current_time = time.time()
    if current_time - st.session_state.last_reload >= 30:
        st.session_state.last_reload = current_time
        st.cache_data.clear()
        st.rerun()

def main():
    st.title("Dashboard de Vendas")
    
    # Chamar auto_reload
    auto_reload()

    # Filtro de Data para Tabela 1
    st.subheader("Filtro de Período (Fornecedores)")
    today = datetime.today()
    col1, col2 = st.columns(2)
    with col1:
        data_inicial = st.date_input(
            "Data Inicial",
            value=datetime(today.year - 1, 1, 1),
            key="data_inicial"
        )
    with col2:
        data_final = st.date_input(
            "Data Final",
            value=today,
            key="data_final"
        )
    
    # Converter para datetime
    data_inicial = datetime.combine(data_inicial, datetime.min.time())
    data_final = datetime.combine(data_final, datetime.max.time())
    
    if data_inicial > data_final:
        st.error("A data inicial não pode ser maior que a data final.")
        return

    # Buscar dados do Supabase
    with st.spinner("Carregando dados do Supabase..."):
        df = get_data_from_supabase(cache, data_inicial, data_final)
    
    if not df.is_empty():
        # --- Primeira Tabela: Valor Total por Fornecedor por Mês ---
        st.subheader("Valor Total por Fornecedor por Mês")
        
        # Barra de Pesquisa
        search_term = st.text_input("Pesquisar Fornecedor:", "", key="search_fornecedor")
        
        # Agrupar por fornecedor, ano e mês
        df_grouped = df.group_by(['FORNECEDOR', 'MES', 'ANO']).agg(
            VALOR_TOTAL_ITEM=pl.col('VALOR_TOTAL_ITEM').sum()
        ).sort(['FORNECEDOR', 'ANO', 'MES'])
        
        # Pivot table
        pivot_df = df_grouped.pivot(
            values='VALOR_TOTAL_ITEM',
            index='FORNECEDOR',
            columns=['ANO', 'MES'],
            aggregate_function='sum'
        ).fill_null(0)
        
        # Renomear colunas
        month_names = {
            1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr',
            5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Ago',
            9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
        }
        new_columns = ['FORNECEDOR'] + [
            f"{month_names.get(int(col[1]), col[1])}-{col[0]}" 
            for col in pivot_df.columns[1:]
        ]
        pivot_df.columns = new_columns
        
        # Adicionar coluna de total
        pivot_df = pivot_df.with_columns(
            Total=pl.sum_horizontal(pl.col(col) for col in pivot_df.columns if col != 'FORNECEDOR')
        )
        
        # Filtrar fornecedores com base na busca
        if search_term:
            pivot_df = pivot_df.filter(
                pl.col('FORNECEDOR').str.contains(search_term, case=False)
            )
        
        # Converter para Pandas para AgGrid
        pivot_df_pandas = pivot_df.to_pandas()
        
        # Verificar se há resultados
        if pivot_df.is_empty():
            st.warning("Nenhum fornecedor encontrado com o termo pesquisado.")
        else:
            # Configurar AgGrid
            gb = GridOptionsBuilder.from_dataframe(pivot_df_pandas)
            gb.configure_default_column(
                sortable=True, filter=True, resizable=True, groupable=False, minWidth=100
            )
            gb.configure_column(
                "FORNECEDOR",
                headerName="Fornecedor",
                pinned="left",
                width=300,
                filter="agTextColumnFilter"
            )
            for col in pivot_df_pandas.columns:
                if col != "FORNECEDOR":
                    gb.configure_column(
                        col,
                        headerName=col,
                        type=["numericColumn"],
                        valueFormatter="x.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'})",
                        cellRenderer="agAnimateShowChangeCellRenderer",
                        width=110
                    )
            
            gb.configure_grid_options(
                enableRangeSelection=True,
                statusBar={
                    "statusPanels": [
                        {"statusPanel": "agTotalRowCountComponent"},
                        {"statusPanel": "agFilteredRowCountComponent"},
                        {"statusPanel": "agAggregationComponent"}
                    ]
                }
            )
            
            AgGrid(
                pivot_df_pandas,
                gridOptions=gb.build(),
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                height=400,
                allow_unsafe_jscode=True,
                theme="streamlit"
            )
            
            # Download CSV
            csv = pivot_df_pandas.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig")
            st.download_button(
                label="Download CSV - Fornecedores",
                data=csv,
                file_name=f'valor_vendas_por_fornecedor_{data_inicial.year}_ate_{data_final.strftime("%Y%m%d")}.csv',
                mime='text/csv',
            )
        
        # --- Segunda Tabela: Quantidade Vendida por Produto por Mês ---
        st.markdown("---")
        st.subheader("Quantidade Vendida por Produto por Mês")
        
        # Filtro de Ano e Mês
        anos = df['ANO'].unique().sort().to_list()
        meses = df['MES'].unique().sort().to_list()
        meses_nomes = [month_names.get(m, str(m)) for m in meses]
        
        # Definir padrões
        current_year = today.year
        current_month = today.month
        current_month_name = month_names.get(current_month, str(current_month))
        
        # Seletor de Ano e Mês
        col1, col2 = st.columns(2)
        with col1:
            selected_ano = st.selectbox(
                "Selecione o Ano",
                anos,
                index=anos.index(current_year) if current_year in anos else 0,
                key="ano_produto"
            )
        with col2:
            selected_mes = st.selectbox(
                "Selecione o Mês",
                meses_nomes,
                index=meses_nomes.index(current_month_name) if current_month_name in meses_nomes else 0,
                key="mes_produto"
            )
        
        # Converter nome do mês para número
        selected_mes_num = list(month_names.keys())[list(month_names.values()).index(selected_mes)] if selected_mes in month_names.values() else int(selected_mes)
        
        # Filtrar dados
        df_filtered = df.filter(
            (pl.col('MES') == selected_mes_num) & (pl.col('ANO') == selected_ano)
        )
        
        if not df_filtered.is_empty():
            # Agrupar por colunas
            pivot_produtos = df_filtered.group_by(
                ['CODPROD', 'PRODUTO', 'CODIGOVENDEDOR', 'VENDEDOR', 'CODCLI', 'CLIENTE', 'FORNECEDOR']
            ).agg(
                QT=pl.col('QT').sum()
            ).sort(['PRODUTO'])
            
            # Reorganizar colunas
            pivot_produtos = pivot_produtos.select([
                'PRODUTO', 'CODPROD', 'VENDEDOR', 'CODIGOVENDEDOR', 'CLIENTE', 'CODCLI', 'FORNECEDOR', 'QT'
            ])
            
            # Converter para Pandas
            pivot_produtos_pandas = pivot_produtos.to_pandas()
            
            # Configurar AgGrid
            gb_produtos = GridOptionsBuilder.from_dataframe(pivot_produtos_pandas)
            gb_produtos.configure_default_column(
                sortable=True, filter=True, resizable=True, groupable=False, minWidth=100
            )
            gb_produtos.configure_column(
                "PRODUTO",
                headerName="Produto",
                pinned="left",
                width=250,
                filter="agTextColumnFilter"
            )
            gb_produtos.configure_column(
                "CODPROD",
                headerName="Cód. Produto",
                pinned="left",
                width=120,
                filter="agTextColumnFilter"
            )
            gb_produtos.configure_column(
                "VENDEDOR",
                headerName="Vendedor",
                width=250,
                filter="agTextColumnFilter"
            )
            gb_produtos.configure_column(
                "CODIGOVENDEDOR",
                headerName="Cód. Vendedor",
                width=120,
                filter="agTextColumnFilter"
            )
            gb_produtos.configure_column(
                "CLIENTE",
                headerName="Cliente",
                width=250,
                filter="agTextColumnFilter"
            )
            gb_produtos.configure_column(
                "CODCLI",
                headerName="Cód. Cliente",
                width=120,
                filter="agTextColumnFilter"
            )
            gb_produtos.configure_column(
                "FORNECEDOR",
                headerName="Fornecedor",
                width=250,
                filter="agTextColumnFilter"
            )
            gb_produtos.configure_column(
                "QT",
                headerName="Quantidade",
                type=["numericColumn"],
                valueFormatter="Math.floor(x).toLocaleString('pt-BR')",
                cellRenderer="agAnimateShowChangeCellRenderer",
                width=120
            )
            
            gb_produtos.configure_grid_options(
                enableRangeSelection=True,
                statusBar={
                    "statusPanels": [
                        {"statusPanel": "agTotalRowCountComponent"},
                        {"statusPanel": "agFilteredRowCountComponent"},
                        {"statusPanel": "agAggregationComponent"}
                    ]
                }
            )
            
            st.write(f"Quantidade vendida por produto para {selected_mes}-{selected_ano}:")
            AgGrid(
                pivot_produtos_pandas,
                gridOptions=gb_produtos.build(),
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                height=400,
                allow_unsafe_jscode=True,
                theme="streamlit"
            )
            
            # Exportar para CSV
            csv_produtos = pivot_produtos_pandas.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig")
            st.download_button(
                label="Download CSV - Produtos",
                data=csv_produtos,
                file_name=f'quantidade_vendida_por_produto_{data_inicial.year}_ate_{data_final.strftime("%Y%m%d")}.csv',
                mime='text/csv',
            )
        else:
            st.warning("Nenhum dado encontrado para o mês e ano selecionados.")
    else:
        st.warning("Nenhum dado encontrado para o período selecionado.")

if __name__ == "__main__":
    main()
