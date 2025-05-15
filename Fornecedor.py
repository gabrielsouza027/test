import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from cachetools import TTLCache
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do cache (TTL de 300 segundos para sincronizar com atualizações)
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
        "filial_filter": "CODFILIAL=in.('1','2')"
    }
}

# Função para buscar página do Supabase com retry
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_supabase_page(table, offset, limit, filter_query=None):
    try:
        query = supabase.table(table).select("*")
        if filter_query:
            query = query.filter(filter_query)
        response = query.range(offset, offset + limit - 1).execute()
        data = response.data
        logger.info(f"Recuperados {len(data)} registros da tabela {table}, offset {offset}")
        return data
    except Exception as e:
        logger.error(f"Erro ao buscar página da tabela {table}, offset {offset}: {e}")
        raise

# Função para buscar dados do Supabase com cache, paginação e paralelismo
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
    filial_filter = config["filial_filter"]

    try:
        all_data = []
        limit = 50000  # Tamanho do lote aumentado
        max_pages = 20  # Limite ajustável

        # Construir filtro de data e filial
        data_inicial_str = data_inicial.strftime('%Y-%m-%d')
        data_final_str = data_final.strftime('%Y-%m-%d')
        filter_query = (
            f"{filial_filter}&{date_column}=gte.{data_inicial_str}&{date_column}=lte.{data_final_str}"
        )

        # Buscar dados em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            offset = 0
            for _ in range(max_pages):
                futures.append(executor.submit(fetch_supabase_page, table, offset, limit, filter_query))
                offset += limit
                if offset >= limit * max_pages:
                    break

            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    if data:
                        all_data.extend(data)
                    else:
                        break
                except Exception as e:
                    logger.error(f"Erro em uma requisição: {e}")
                    continue

        if all_data:
            df = pd.DataFrame(all_data)
            missing_columns = [col for col in columns_expected if col not in df.columns]
            if missing_columns:
                logger.error(f"Colunas ausentes na tabela {table}: {missing_columns}")
                st.error(f"Colunas ausentes na tabela {table}: {missing_columns}")
                _cache[key] = pd.DataFrame()
                return pd.DataFrame()

            # Converter DATA para datetime e extrair mês e ano
            df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
            df = df.dropna(subset=['DATA'])
            df['MES'] = df['DATA'].dt.month
            df['ANO'] = df['DATA'].dt.year

            # Garantir tipos numéricos
            df['QT'] = pd.to_numeric(df['QT'], errors='coerce').fillna(0)
            df['PVENDA'] = pd.to_numeric(df['PVENDA'], errors='coerce').fillna(0)

            # Calcular valor total
            df['VALOR_TOTAL_ITEM'] = df['QT'] * df['PVENDA']

            _cache[key] = df
            logger.info(f"Dados carregados com sucesso: {len(df)} registros")
        else:
            logger.warning(f"Nenhum dado retornado da tabela {table}")
            st.warning(f"Nenhum dado retornado da tabela {table} para o período selecionado.")
            _cache[key] = pd.DataFrame()
            df = pd.DataFrame()

    except Exception as e:
        logger.error(f"Erro ao buscar dados da tabela {table}: {e}")
        st.error(f"Erro ao buscar dados: {e}")
        _cache[key] = pd.DataFrame()
        df = pd.DataFrame()

    return _cache[key]

# Função para realizar o reload automático a cada 30 segundos
def auto_reload():
    if 'last_reload' not in st.session_state:
        st.session_state.last_reload = time.time()
    
    current_time = time.time()
    if current_time - st.session_state.last_reload >= 30:  # 30 segundos
        st.session_state.last_reload = current_time
        st.cache_data.clear()  # Limpar o cache para forçar nova busca
        st.rerun()  # Forçar reload da página

def main():
    st.title("Dashboard de Vendas")
    
    # Chamar auto_reload para atualizações frequentes
    auto_reload()

    # Filtro de Data para Tabela 1
    st.subheader("Filtro de Período (Fornecedores)")
    today = datetime.today()
    col1, col2 = st.columns(2)
    with col1:
        data_inicial = st.date_input(
            "Data Inicial",
            value=datetime(today.year - 1, 1, 1),  # Um ano antes do dia atual
            key="data_inicial"
        )
    with col2:
        data_final = st.date_input(
            "Data Final",
            value=today,  # Dia atual
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
    
    if not df.empty:
        # --- Primeira Tabela: Valor Total por Fornecedor por Mês ---
        st.subheader("Valor Total por Fornecedor por Mês")
        
        # Barra de Pesquisa
        search_term = st.text_input("Pesquisar Fornecedor:", "", key="search_fornecedor")
        
        # Agrupar por fornecedor, ano e mês
        df_grouped = df.groupby(['FORNECEDOR', 'MES', 'ANO'])['VALOR_TOTAL_ITEM'].sum().reset_index()
        
        # Pivot table para fornecedores como linhas e meses como colunas
        pivot_df = df_grouped.pivot_table(
            values='VALOR_TOTAL_ITEM',
            index='FORNECEDOR',
            columns=['ANO', 'MES'],
            aggfunc='sum',
            fill_value=0
        )
        
        # Renomear colunas para incluir ano e nome do mês
        month_names = {
            1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr',
            5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Ago',
            9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
        }
        new_columns = [
            f"{month_names[month]}-{year}" 
            for year, month in pivot_df.columns
        ]
        pivot_df.columns = new_columns
        
        # Adicionar coluna de total
        pivot_df['Total'] = pivot_df.sum(axis=1)
        
        # Resetar índice para tornar FORNECEDOR uma coluna
        pivot_df = pivot_df.reset_index()
        
        # Filtrar fornecedores com base na busca
        if search_term:
            pivot_df = pivot_df[
                pivot_df['FORNECEDOR'].str.contains(search_term, case=False, na=False)
            ]
        
        # Verificar se há resultados após o filtro
        if pivot_df.empty:
            st.warning("Nenhum fornecedor encontrado com o termo pesquisado.")
        else:
            # Configurar opções do AgGrid
            gb = GridOptionsBuilder.from_dataframe(pivot_df)
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
            for col in pivot_df.columns:
                if col != "FORNECEDOR":
                    gb.configure_column(
                        col,
                        headerName=col,
                        type=["numericColumn"],
                        valueFormatter="x.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'})",
                        cellRenderer="agAnimateShowChangeCellRenderer",
                        width=110
                    )
            
            # Habilitar linha de totais
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
            
            # Renderizar AgGrid
            AgGrid(
                pivot_df,
                gridOptions=gb.build(),
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                height=400,
                allow_unsafe_jscode=True,
                theme="streamlit"
            )
            
            # Download CSV
            csv = pivot_df.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig")
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
        anos = sorted(df['ANO'].unique())
        meses = sorted(df['MES'].unique())
        meses_nomes = [month_names[m] for m in meses]
        
        # Definir padrões para ano e mês (atual)
        current_year = today.year
        current_month = today.month
        current_month_name = month_names[current_month]
        
        # Seletor de Ano e Mês
        col1, col2 = st.columns(2)
        with col1:
            selected_ano = st.selectbox("Selecione o Ano", anos, index=anos.index(current_year) if current_year in anos else 0, key="ano_produto")
        with col2:
            selected_mes = st.selectbox("Selecione o Mês", meses_nomes, index=meses_nomes.index(current_month_name) if current_month_name in meses_nomes else 0, key="mes_produto")
        
        # Converter nome do mês de volta para número
        selected_mes_num = list(month_names.keys())[list(month_names.values()).index(selected_mes)]
        
        # Filtrar os dados com base no ano e mês selecionados
        df_filtered = df[
            (df['MES'] == selected_mes_num) & (df['ANO'] == selected_ano)
        ]
        
        if not df_filtered.empty:
            # Agrupar por colunas otimizadas
            pivot_produtos = df_filtered.groupby(
                ['CODPROD', 'PRODUTO', 'CODIGOVENDEDOR', 'VENDEDOR', 'CODCLI', 'CLIENTE', 'FORNECEDOR']
            )['QT'].sum().reset_index()
            
            # Reorganizar colunas para melhor apresentação
            pivot_produtos = pivot_produtos[
                ['PRODUTO', 'CODPROD', 'VENDEDOR', 'CODIGOVENDEDOR', 'CLIENTE', 'CODCLI', 'FORNECEDOR', 'QT']
            ]
            
            # Configurar AgGrid para exibir colunas de forma otimizada
            gb_produtos = GridOptionsBuilder.from_dataframe(pivot_produtos)
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
            
            # Habilitar linha de totais
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
            
            # Exibir tabela
            st.write(f"Quantidade vendida por produto para {selected_mes}-{selected_ano}:")
            AgGrid(
                pivot_produtos,
                gridOptions=gb_produtos.build(),
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                height=400,
                allow_unsafe_jscode=True,
                theme="streamlit"
            )
            
            # Exportar para CSV
            csv_produtos = pivot_produtos.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig")
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
