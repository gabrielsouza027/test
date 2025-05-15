import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, date
import time
from cachetools import TTLCache
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Injetar CSS para estilização
st.markdown("""
    <style>
    .stApp {
        max-width: 100% !important;
    }
    </style>
""", unsafe_allow_html=True)

# Configuração do cliente Supabase usando secrets do Streamlit Cloud
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except KeyError as e:
    st.error(f"Erro: Variável {e} não encontrada no secrets.toml. Verifique a configuração no Streamlit Cloud.")
    st.stop()

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Erro: SUPABASE_URL ou SUPABASE_KEY não estão definidos.")
    st.stop()

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    st.error(f"Erro ao inicializar o cliente Supabase: {e}")
    st.stop()

cache = TTLCache(maxsize=10, ttl=300)

SUPABASE_CONFIG = {
    "pedidos": {
        "table": "PCPEDI",
        "columns": ['created_at', 'NUMPED', 'NUMCAR', 'DATA', 'CODCLI', 'QT', 'CODPROD', 'PVENDA', 
                    'POSICAO', 'CLIENTE', 'DESCRICAO_PRODUTO', 'CODIGO_VENDEDOR', 'NOME_VENDEDOR', 'NUMNOTA', 
                    'OBS', 'OBS1', 'OBS2', 'CODFILIAL', 'MUNICIPIO', 'DESCRICAO_ROTA']
    }
}

@st.cache_data(show_spinner=False, ttl=60)
def fetch_pedidos(_cache, table, columns_expected, data_inicial, data_final):
    key = f"{table}_{data_inicial}_{data_final}"
    if key in _cache:
        logger.info(f"Dados da tabela {table} recuperados do cache")
        return _cache[key]

    try:
        data_inicial_str = data_inicial.strftime("%Y-%m-%d")
        data_final_str = data_final.strftime("%Y-%m-%d")
        
        all_data = []
        offset = 0
        limit = 1000

        while True:
            response = supabase.table(table).select("*").gte("DATA", data_inicial_str).lte("DATA", data_final_str).range(offset, offset + limit - 1).execute()
            data = response.data
            if not data:
                break
            all_data.extend(data)
            offset += limit

        df = pd.DataFrame(all_data)

        if df.empty:
            _cache[key] = df
            return df

        df = df[columns_expected]
        df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
        df['QT'] = pd.to_numeric(df['QT'], errors='coerce').fillna(0)
        df['PVENDA'] = pd.to_numeric(df['PVENDA'], errors='coerce').fillna(0)
        df['valor_total'] = df['QT'] * df['PVENDA']
        _cache[key] = df
        return df

    except Exception as e:
        st.error(f"Erro ao buscar dados: {e}")
        return pd.DataFrame()


def main():
    st.title("Pedidos de Venda")

    data_inicial = st.date_input("Data Inicial", date.today())
    data_final = st.date_input("Data Final", date.today())

    config = SUPABASE_CONFIG["pedidos"]
    df = fetch_pedidos(cache, config["table"], config["columns"], data_inicial, data_final)

    if df.empty:
        st.warning("Sem dados para o período selecionado.")
        return

    # Agrupamento e Filtros
    df_grouped = df.groupby(['NUMPED', 'NUMCAR', 'DATA', 'CODCLI', 'CLIENTE', 'CODIGO_VENDEDOR', 'NOME_VENDEDOR',
                             'POSICAO', 'NUMNOTA', 'OBS', 'OBS1', 'OBS2', 'CODFILIAL', 'MUNICIPIO', 'DESCRICAO_ROTA']).agg({
        'QT': 'sum',
        'PVENDA': 'sum',
        'DESCRICAO_PRODUTO': lambda x: ', '.join(x.astype(str).unique())
    }).reset_index()

    clientes_unicos = sorted(df_grouped['CLIENTE'].dropna().unique())
    vendedores_unicos = sorted(df_grouped['NOME_VENDEDOR'].dropna().unique())
    filiais_unicas = sorted(df_grouped['CODFILIAL'].dropna().unique())
    status_unicos = sorted(df_grouped['POSICAO'].dropna().unique())
    regioes_unicas = sorted(df_grouped['DESCRICAO_ROTA'].dropna().unique())

    cliente_filtro = st.multiselect("Clientes", clientes_unicos)
    vendedor_filtro = st.multiselect("Vendedores", vendedores_unicos)
    filial_filtro = st.multiselect("Filiais", filiais_unicas)
    status_filtro = st.multiselect("Status", status_unicos)
    regiao_filtro = st.multiselect("Regiões", regioes_unicas)

    if cliente_filtro:
        df_grouped = df_grouped[df_grouped['CLIENTE'].isin(cliente_filtro)]
    if vendedor_filtro:
        df_grouped = df_grouped[df_grouped['NOME_VENDEDOR'].isin(vendedor_filtro)]
    if filial_filtro:
        df_grouped = df_grouped[df_grouped['CODFILIAL'].isin(filial_filtro)]
    if status_filtro:
        df_grouped = df_grouped[df_grouped['POSICAO'].isin(status_filtro)]
    if regiao_filtro:
        df_grouped = df_grouped[df_grouped['DESCRICAO_ROTA'].isin(regiao_filtro)]

    st.dataframe(df_grouped)


if __name__ == "__main__":
    main()
