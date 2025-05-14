import streamlit as st
import pandas as pd
from supabase import create_client, Client
import datetime
from cachetools import TTLCache
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import time
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração dos caches (TTL de 60 segundos)
cache_vendas = TTLCache(maxsize=1, ttl=60)
cache_estoque = TTLCache(maxsize=1, ttl=60)

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
        "columns": ["CODPROD", "QT", "DESCRICAO_1", "DESCRICAO_2", "DATA"]
    },
    "estoque": {
        "table": "ESTOQUE",
        "columns": ["CODFILIAL", "CODPROD", "QT_ESTOQUE", "QTULTENT", "DTULTENT", "DTULTSAIDA", "QTRESERV", 
                    "QTINDENIZ", "DTULTPEDCC", "BLOQUEADA", "NOME_PROD"]
    }
    # Adicione mais tabelas aqui, se necessário
    # "outra_tabela": {
    #     "table": "NOME_DA_TABELA",
    #     "columns": ["COLUNA1", "COLUNA2", ...]
    # }
}

# Função para buscar dados do Supabase com paginação
@st.cache_data(show_spinner=False, ttl=60)
def fetch_supabase_data(_cache, table, columns_expected, date_column=None):
    key = f"{table}"
    if key in _cache:
        logger.info(f"Dados da tabela {table} recuperados do cache")
        return _cache[key]

    try:
        all_data = []
        offset = 0
        limit = 1000  # Limite por página do Supabase

        while True:
            response = supabase.table(table).select("*").range(offset, offset + limit - 1).execute()
            data = response.data
            if not data:
                logger.info(f"Finalizada a recuperação de dados da tabela {table}")
                break
            all_data.extend(data)
            offset += limit
            logger.info(f"Recuperados {len(data)} registros da tabela {table}, total até agora: {len(all_data)}")

        if all_data:
            df = pd.DataFrame(all_data)
            missing_columns = [col for col in columns_expected if col not in df.columns]
            if missing_columns:
                logger.error(f"Colunas ausentes na tabela {table}: {missing_columns}")
                st.error(f"Colunas ausentes na tabela {table}: {missing_columns}")
                _cache[key] = pd.DataFrame()
                return pd.DataFrame()
            if date_column and date_column in df.columns:
                df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
                df = df.dropna(subset=[date_column])
            _cache[key] = df
            logger.info(f"Dados carregados com sucesso da tabela {table}: {len(df)} registros")
        else:
            logger.warning(f"Nenhum dado retornado da tabela {table}")
            st.warning(f"Nenhum dado retornado da tabela {table}.")
            _cache[key] = pd.DataFrame()
            df = pd.DataFrame()

    except Exception as e:
        logger.error(f"Erro ao buscar dados da tabela {table}: {e}")
        st.error(f"Erro ao buscar dados da tabela {table}: {e}")
        _cache[key] = pd.DataFrame()
        df = pd.DataFrame()

    return _cache[key]

# Função para buscar dados de vendas (VWSOMELIER)
def fetch_vendas_data():
    config = SUPABASE_CONFIG["vendas"]
    df = fetch_supabase_data(cache_vendas, config["table"], config["columns"], date_column="DATA")
    if not df.empty:
        df['QT'] = pd.to_numeric(df['QT'], errors='coerce').fillna(0)
    return df

# Função para buscar dados de estoque (ESTOQUE)
def fetch_estoque_data():
    config = SUPABASE_CONFIG["estoque"]
    df = fetch_supabase_data(cache_estoque, config["table"], config["columns"])
    if not df.empty:
        for col in ['QTULTENT', 'QT_ESTOQUE', 'QTRESERV', 'QTINDENIZ', 'BLOQUEADA']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        for col in ['DTULTENT', 'DTULTSAIDA', 'DTULTPEDCC']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

# Função para realizar o reload automático a cada 1 minuto
def auto_reload():
    if 'last_reload' not in st.session_state:
        st.session_state.last_reload = time.time()
    
    current_time = time.time()
    if current_time - st.session_state.last_reload >= 60:  # 60 segundos
        st.session_state.last_reload = current_time
        st.cache_data.clear()  # Limpar o cache para forçar nova busca
        st.rerun()  # Forçar reload da página

# Função principal
def main():
    st.title("📦 Análise de Estoque e Vendas")
    st.markdown("Análise dos produtos vendidos e estoque disponível.")

    # Chamar auto_reload para verificar se precisa atualizar
    auto_reload()

    # Definir as datas de início e fim para os últimos 2 meses
    data_final = datetime.date.today()  # 13/05/2025
    data_inicial = data_final - datetime.timedelta(days=60)  # 14/03/2025

    # Buscar dados de vendas (VWSOMELIER)
    with st.spinner("Carregando dados de vendas..."):
        vendas_df = fetch_vendas_data()

    if vendas_df.empty:
        st.warning("Não há vendas para o período selecionado.")
    else:
        # Agrupar as vendas por produto e somar as quantidades vendidas
        vendas_grouped = vendas_df.groupby('CODPROD')['QT'].sum().reset_index()

    # Buscar dados de estoque (ESTOQUE)
    with st.spinner("Carregando dados de estoque..."):
        estoque_df = fetch_estoque_data()

    if estoque_df.empty:
        st.warning("Não há dados de estoque para o período selecionado.")
    else:
        # Verificar se os produtos com alta venda estão sem estoque
        merged_df = pd.merge(vendas_grouped, estoque_df[['CODPROD', 'NOME_PROD', 'QT_ESTOQUE']], on='CODPROD', how='left')

        # Filtrando os produtos que NÃO possuem estoque
        sem_estoque_df = merged_df[merged_df['QT_ESTOQUE'].isna() | (merged_df['QT_ESTOQUE'] <= 0)]

        # Barra de pesquisa para código do produto
        pesquisar = st.text_input("Pesquisar por Código do Produto ou Nome", "")

        # Renomear as colunas
        df = estoque_df.copy()
        df = df.rename(columns={
            'CODFILIAL': 'Código da Filial',
            'CODPROD': 'Código do Produto',
            'NOME_PROD': 'Nome do Produto',
            'QTULTENT': 'Quantidade Última Entrada',
            'QT_ESTOQUE': 'Estoque Disponível',
            'QTRESERV': 'Quantidade Reservada',
            'QTINDENIZ': 'Quantidade Avariada',
            'DTULTENT': 'Data Última Entrada',
            'DTULTSAIDA': 'Data Última Saída',
            'DTULTPEDCC': 'Data Último Pedido Compra',
            'BLOQUEADA': 'Quantidade Bloqueada'
        })

        if pesquisar:
            df = df[
                (df['Código do Produto'].astype(str).str.contains(pesquisar, case=False, na=False)) |
                (df['Nome do Produto'].str.contains(pesquisar, case=False, na=False))
            ]

        df['Quantidade Total'] = df[['Estoque Disponível', 'Quantidade Reservada', 'Quantidade Bloqueada']].fillna(0).sum(axis=1)

        # Reordenar as colunas
        df = df.reindex(columns=[
            'Código da Filial', 'Código do Produto', 'Nome do Produto', 'Estoque Disponível', 'Quantidade Reservada',
            'Quantidade Bloqueada', 'Quantidade Avariada', 'Quantidade Total', 'Quantidade Última Entrada',
            'Data Última Entrada', 'Data Última Saída', 'Data Último Pedido Compra'
        ])

        # Configurar a tabela de estoque com AgGrid e larguras fixas
        st.subheader("✅ Estoque")
        st.markdown("Use a barra de rolagem para ver mais linhas.")
        gb = GridOptionsBuilder.from_dataframe(df)
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

        # Formatar números e datas para exibição
        df_display = df.copy()
        for col in ['Estoque Disponível', 'Quantidade Reservada', 'Quantidade Bloqueada', 'Quantidade Avariada', 'Quantidade Total', 'Quantidade Última Entrada']:
            df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        for col in ['Data Última Entrada', 'Data Última Saída', 'Data Último Pedido Compra']:
            df_display[col] = df_display[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else "")

        AgGrid(
            df_display,
            gridOptions=grid_options,
            update_mode=GridUpdateMode.NO_UPDATE,
            allow_unsafe_jscode=True,
            height=400,
            theme='streamlit',
            fit_columns_on_grid_load=False
        )

        if sem_estoque_df.empty:
            st.info("Não há produtos vendidos sem estoque.")
        else:
            # Exibir a tabela com os produtos sem estoque mas vendidos
            st.subheader("❌ Produtos Sem Estoque com Venda nos Últimos 2 Meses")

            sem_estoque_df_renomeado = sem_estoque_df[sem_estoque_df['QT_ESTOQUE'].isna() | (sem_estoque_df['QT_ESTOQUE'] <= 0)]

            sem_estoque_df_renomeado = sem_estoque_df_renomeado.rename(columns={
                'CODPROD': 'CÓDIGO PRODUTO',
                'NOME_PROD': 'NOME DO PRODUTO',
                'QT': 'QUANTIDADE VENDIDA',
                'QT_ESTOQUE': 'ESTOQUE TOTAL'
            })

            sem_estoque_df_renomeado = sem_estoque_df_renomeado[
                sem_estoque_df_renomeado['NOME DO PRODUTO'].notna() & 
                (sem_estoque_df_renomeado['NOME DO PRODUTO'] != '')
            ]

            sem_estoque_df_renomeado = sem_estoque_df_renomeado[[
                'CÓDIGO PRODUTO', 'NOME DO PRODUTO', 'QUANTIDADE VENDIDA', 'ESTOQUE TOTAL'
            ]]

            gb = GridOptionsBuilder.from_dataframe(sem_estoque_df_renomeado)
            gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=False)
            gb.configure_column("CÓDIGO PRODUTO", width=150)
            gb.configure_column("NOME DO PRODUTO", width=300)
            gb.configure_column("QUANTIDADE VENDIDA", width=200)
            gb.configure_column("ESTOQUE TOTAL", width=200)
            gb.configure_pagination(enabled=False)
            gb.configure_grid_options(domLayout='normal')
            grid_options = gb.build()

            df_sem_estoque_display = sem_estoque_df_renomeado.copy()
            df_sem_estoque_display['QUANTIDADE VENDIDA'] = pd.to_numeric(df_sem_estoque_display['QUANTIDADE VENDIDA'], errors='coerce').fillna(0)
            df_sem_estoque_display['ESTOQUE TOTAL'] = pd.to_numeric(df_sem_estoque_display['ESTOQUE TOTAL'], errors='coerce').fillna(0)
            df_sem_estoque_display['QUANTIDADE VENDIDA'] = df_sem_estoque_display['QUANTIDADE VENDIDA'].apply(lambda x: f"{x:,.0f}")
            df_sem_estoque_display['ESTOQUE TOTAL'] = df_sem_estoque_display['ESTOQUE TOTAL'].apply(lambda x: f"{x:,.0f}")

            AgGrid(
                df_sem_estoque_display,
                gridOptions=grid_options,
                update_mode=GridUpdateMode.NO_UPDATE,
                allow_unsafe_jscode=True,
                height=300,
                theme='streamlit',
                fit_columns_on_grid_load=True
            )

if __name__ == "__main__":
    main()
