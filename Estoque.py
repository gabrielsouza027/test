import streamlit as st
import pandas as pd
from supabase import create_client, Client, ClientOptions
import datetime
import logging
import time
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EstoqueApp")

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

# Inicializar cliente Supabase com timeout
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
    },
    "estoque": {
        "table": "ESTOQUE",
        "columns": ["CODFILIAL", "CODPROD", "QT_ESTOQUE", "QTULTENT", "DTULTENT", "DTULTSAIDA", "QTRESERV",
                    "QTINDENIZ", "DTULTPEDCOMPRA", "BLOQUEADA", "NOME_PRODUTO"],
        "date_column": "DTULTENT"
    }
}

def fetch_supabase_page(table, offset, limit, filter_query=None):
    """Busca uma página de dados do Supabase."""
    try:
        query = supabase.table(table).select("*")
        if filter_query:
            for filtro in filter_query:
                column, operator, value = filtro
                if operator == "in":
                    query = query.in_(column, value)
                else:
                    query = query.filter(column, operator, value)
        response = query.range(offset, offset + limit - 1).execute()
        data = response.data
        logger.info(f"Recuperados {len(data)} registros da tabela {table}, offset {offset}")
        return data
    except Exception as e:
        logger.error(f"Erro ao buscar página da tabela {table}, offset {offset}: {e}")
        raise

@st.cache_data(show_spinner=False, ttl=900)
def fetch_supabase_data(table, columns_expected, date_column=None, last_update=None):
    """Busca dados do Supabase com cache."""
    key = f"{table}_{last_update or 'full'}"
    logger.info(f"Buscando dados da tabela {table}, chave: {key}")

    try:
        all_data = []
        limit = 100  # Aumentado para reduzir número de requisições
        max_pages = 10  # Ajustado para evitar excesso de chamadas
        filters = []

        if last_update and date_column:
            filters.append((date_column, "gte", last_update.strftime('%Y-%m-%d')))

        for offset in range(0, limit * max_pages, limit):
            data = fetch_supabase_page(table, offset, limit, filters)
            if not data:
                break
            all_data.extend(data)

        if not all_data:
            logger.warning(f"Nenhum dado retornado da tabela {table}")
            st.warning(f"Nenhum dado retornado da tabela {table}.")
            return pd.DataFrame()

        df = pd.DataFrame(all_data)
        missing_columns = [col for col in columns_expected if col not in df.columns]
        if missing_columns:
            st.error(f"Colunas ausentes na tabela {table}: {missing_columns}")
            logger.error(f"Colunas ausentes: {missing_columns}")
            return pd.DataFrame()

        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            df = df.dropna(subset=[date_column])

        logger.info(f"Dados carregados com sucesso da tabela {table}: {len(df)} registros")
        return df

    except Exception as e:
        st.error(f"Erro ao buscar dados da tabela {table}: {e}")
        logger.error(f"Erro geral: {e}")
        return pd.DataFrame()

def fetch_vendas_data():
    """Busca dados de vendas."""
    config = SUPABASE_CONFIG["vendas"]
    last_update = st.session_state.get('last_vendas_update', None)
    df = fetch_supabase_data(
        table=config["table"],
        columns_expected=config["columns"],
        date_column=config["date_column"],
        last_update=last_update
    )
    return df

def fetch_estoque_data():
    """Busca dados de estoque."""
    config = SUPABASE_CONFIG["estoque"]
    last_update = st.session_state.get('last_estoque_update', None)
    df = fetch_supabase_data(
        table=config["table"],
        columns_expected=config["columns"],
        date_column=config["date_column"],
        last_update=last_update
    )
    if not df.empty:
        for col in ['QTULTENT', 'QT_ESTOQUE', 'QTRESERV', 'QTINDENIZ', 'BLOQUEADA']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        for col in ['DTULTENT', 'DTULTSAIDA', 'DTULTPEDCOMPRA']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        if config["date_column"] in df.columns:
            st.session_state['last_estoque_update'] = df[config["date_column"]].max()
    return df

def auto_reload():
    """Recarrega os dados automaticamente a cada 10 minutos."""
    if 'last_reload' not in st.session_state:
        st.session_state.last_reload = time.time()
    current_time = time.time()
    if current_time - st.session_state.last_reload >= 600:  # 10 minutos
        st.session_state.last_reload = current_time
        st.cache_data.clear()
        st.rerun()

def main():
    st.title("📦 Análise de Estoque e Vendas")
    st.markdown("Análise dos produtos vendidos e estoque disponível.")

    data_inicial = data_final - datetime.timedelta(days=370)
    data_final = datetime.date.today()
   

    with st.spinner("Carregando dados de vendas..."):
        vendas_df = fetch_vendas_data()

    if vendas_df.empty:
        st.warning("Não há vendas para o período selecionado.")
    else:
        vendas_grouped = vendas_df.groupby('CODPROD')['QT'].sum().reset_index()

    with st.spinner("Carregando dados de estoque..."):
        estoque_df = fetch_estoque_data()

    if estoque_df.empty:
        st.warning("Não há dados de estoque para o período selecionado.")
    else:
        merged_df = pd.merge(vendas_grouped, estoque_df[['CODPROD', 'NOME_PRODUTO', 'QT_ESTOQUE']], on='CODPROD', how='left')
        sem_estoque_df = merged_df[merged_df['QT_ESTOQUE'].isna() | (merged_df['QT_ESTOQUE'] <= 0)]

        pesquisar = st.text_input("Pesquisar por Código do Produto ou Nome", "")

        df = estoque_df.copy()
        df = df.rename(columns={
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
            df = df[
                (df['Código do Produto'].astype(str).str.contains(pesquisar, case=False, na=False)) |
                (df['Nome do Produto'].str.contains(pesquisar, case=False, na=False))
            ]

        df['Quantidade Total'] = df[['Estoque Disponível', 'Quantidade Reservada', 'Quantidade Bloqueada']].fillna(0).sum(axis=1)

        df = df.reindex(columns=[
            'Código da Filial', 'Código do Produto', 'Nome do Produto', 'Estoque Disponível', 'Quantidade Reservada',
            'Quantidade Bloqueada', 'Quantidade Avariada', 'Quantidade Total', 'Quantidade Última Entrada',
            'Data Última Entrada', 'Data Última Saída', 'Data Último Pedido Compra'
        ])

        st.subheader("✅ Estoque")
        st.markdown("Use a barra de rolagem para ver mais linhas.")
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=False)
        gb.configure_pagination(enabled=False)
        gb.configure_grid_options(domLayout='normal')
        grid_options = gb.build()

        df_display = df.copy()
        for col in ['Estoque Disponível', 'Quantidade Reservada', 'Quantidade Bloqueada', 'Quantidade Avariada', 'Quantidade Total', 'Quantidade Última Entrada']:
            df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        for col in ['Data Última Entrada', 'Data Última Saída', 'Data Último Pedido Compra']:
            df_display[col] = df_display[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else "")

        AgGrid(df_display, gridOptions=grid_options, update_mode=GridUpdateMode.NO_UPDATE, allow_unsafe_jscode=True, height=400, theme='streamlit')

        if sem_estoque_df.empty:
            st.info("Não há produtos vendidos sem estoque.")
        else:
            st.subheader("❌ Produtos Sem Estoque com Venda nos Últimos 2 Meses")

            sem_estoque_df_renomeado = sem_estoque_df[sem_estoque_df['QT_ESTOQUE'].isna() | (sem_estoque_df['QT_ESTOQUE'] <= 0)]
            sem_estoque_df_renomeado = sem_estoque_df_renomeado.rename(columns={
                'CODPROD': 'CÓDIGO PRODUTO',
                'NOME_PRODUTO': 'NOME DO PRODUTO',
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
            gb.configure_grid_options(domLayout='normal')
            grid_options = gb.build()

            df_sem_estoque_display = sem_estoque_df_renomeado.copy()
            df_sem_estoque_display['QUANTIDADE VENDIDA'] = pd.to_numeric(df_sem_estoque_display['QUANTIDADE VENDIDA'], errors='coerce').fillna(0)
            df_sem_estoque_display['ESTOQUE TOTAL'] = pd.to_numeric(df_sem_estoque_display['ESTOQUE TOTAL'], errors='coerce').fillna(0)
            df_sem_estoque_display['QUANTIDADE VENDIDA'] = df_sem_estoque_display['QUANTIDADE VENDIDA'].apply(lambda x: f"{x:,.0f}")
            df_sem_estoque_display['ESTOQUE TOTAL'] = df_sem_estoque_display['ESTOQUE TOTAL'].apply(lambda x: f"{x:,.0f}")

            AgGrid(df_sem_estoque_display, gridOptions=grid_options, update_mode=GridUpdateMode.NO_UPDATE, allow_unsafe_jscode=True, height=300, theme='streamlit')

if __name__ == "__main__":
    main()
