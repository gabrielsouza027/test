import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from cachetools import TTLCache
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import os

# Configuração do cache
cache = TTLCache(maxsize=1, ttl=180)

# Conexão com o Supabase usando st.secrets
@st.cache_resource
def init_connection():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
    except KeyError as e:
        st.error(f"Erro: Variável {e} não encontrada no secrets.toml. Verifique a configuração no Streamlit Cloud.")
        return None

    if not url or not key:
        st.error("As variáveis SUPABASE_URL e SUPABASE_KEY devem estar configuradas no secrets.toml.")
        return None

    try:
        return create_client(url, key)
    except Exception as e:
        st.error(f"Erro ao inicializar o cliente Supabase: {e}")
        return None

supabase: Client = init_connection()
if supabase is None:
    st.stop()

# Função para buscar todos os dados
def get_all_data_from_supabase():
    if "all_data" not in cache:
        try:
            all_data = []
            page_size = 1000
            offset = 0

            while True:
                response = (
                    supabase.table("PCVENDEDOR2")
                    .select("*")
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
                data_page = response.data
                if not data_page:
                    break

                all_data.extend(data_page)
                offset += page_size

            if not all_data:
                st.warning("Nenhum dado retornado do Supabase.")
                return pd.DataFrame()

            df = pd.DataFrame(all_data)

            required_columns = ['DATA', 'QT', 'PVENDA', 'FORNECEDOR', 'VENDEDOR', 'CLIENTE', 'PRODUTO', 'CODPROD', 'CODIGOVENDEDOR', 'CODCLI']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                st.error(f"Colunas ausentes no conjunto de dados: {missing_columns}")
                return pd.DataFrame()

            df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
            if df['DATA'].isna().any():
                st.warning("Algumas datas não puderam ser convertidas e serão ignoradas.")
                df = df.dropna(subset=['DATA'])

            df['MES'] = df['DATA'].dt.month
            df['ANO'] = df['DATA'].dt.year
            df['VALOR_TOTAL_ITEM'] = df['QT'] * df['PVENDA']

            cache["all_data"] = df
        except Exception as e:
            st.error(f"Erro ao buscar dados do Supabase: {e}")
            cache["all_data"] = pd.DataFrame()

    return cache["all_data"]

def main():
    st.title("📊 Dashboard de Vendas")

    today = datetime.today()
    data_inicial_default = datetime(today.year, today.month, 1)
    data_final_default = today

    st.subheader("Filtro de Período (Fornecedores)")
    col1, col2 = st.columns(2)
    with col1:
        data_inicial = st.date_input("Data Inicial", value=data_inicial_default, key="data_inicial")
    with col2:
        data_final = st.date_input("Data Final", value=data_final_default, key="data_final")

    data_inicial = datetime.combine(data_inicial, datetime.min.time())
    data_final = datetime.combine(data_final, datetime.max.time())

    if data_inicial > data_final:
        st.error("A data inicial não pode ser maior que a data final.")
        return

    df = get_all_data_from_supabase()
    if df.empty:
        st.warning("Nenhum dado disponível no Supabase. Verifique a conexão ou a tabela.")
        return

    df_filtered = df[(df['DATA'] >= data_inicial) & (df['DATA'] <= data_final)]
    if df_filtered.empty:
        st.warning(f"Nenhum dado encontrado para o período de {data_inicial.strftime('%d/%m/%Y')} a {data_final.strftime('%d/%m/%Y')}.")
        st.write("Intervalo de datas disponível nos dados:")
        st.write(f"Data mínima: {df['DATA'].min().strftime('%d/%m/%Y')}")
        st.write(f"Data máxima: {df['DATA'].max().strftime('%d/%m/%Y')}")
        return

    # TABELA 1
    st.subheader("Valor Total por Fornecedor por Mês")
    search_term = st.text_input("Pesquisar Fornecedor:", "", key="search_fornecedor")
    
    all_months = pd.date_range(start=data_inicial, end=data_final, freq='MS')
    month_names = {
        1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }
    all_month_cols = [f"{month_names[d.month]}-{d.year}" for d in all_months]
    
    df_filtered['ANO'] = df_filtered['DATA'].dt.year
    df_filtered['MES'] = df_filtered['DATA'].dt.month
    df_grouped = df_filtered.groupby(['FORNECEDOR', 'ANO', 'MES'])['VALOR_TOTAL_ITEM'].sum().reset_index()
    df_grouped['MES_ANO'] = df_grouped.apply(lambda row: f"{month_names[row['MES']]}-{row['ANO']}", axis=1)
    
    pivot_df = df_grouped.pivot(index='FORNECEDOR', columns='MES_ANO', values='VALOR_TOTAL_ITEM').fillna(0)
    
    for col in all_month_cols:
        if col not in pivot_df.columns:
            pivot_df[col] = 0
    
    pivot_df = pivot_df[all_month_cols]
    pivot_df['Total'] = pivot_df.sum(axis=1)
    pivot_df = pivot_df.reset_index()
    
    if search_term:
        pivot_df = pivot_df[pivot_df['FORNECEDOR'].str.contains(search_term, case=False, na=False)]
    
    st.markdown(
        """
        <style>
        .ag-root-wrapper {
            width: 1000px !important;
            max-width: 1000px !important;
            margin: 0 auto;
        }
        .ag-header, .ag-body-viewport {
            width: 100% !important;
        }
        .ag-theme-streamlit {
            --ag-grid-size: 6px;
            --ag-header-height: 40px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )
    
    if pivot_df.empty:
        st.warning("Nenhum fornecedor encontrado com o termo pesquisado.")
    else:
        gb = GridOptionsBuilder.from_dataframe(pivot_df)
        gb.configure_default_column(sortable=True, filter=True, resizable=False, flex=1)
        gb.configure_column("FORNECEDOR", headerName="Fornecedor", pinned="left", width=200)
    
        for col in pivot_df.columns:
            if col != "FORNECEDOR":
                gb.configure_column(
                    col,
                    type=["numericColumn"],
                    valueFormatter="x.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'})",
                    cellRenderer="agAnimateShowChangeCellRenderer",
                    flex=1
                )
    
        gb.configure_grid_options(
            domLayout='normal',
            suppressHorizontalScroll=False,
            autoSizeStrategy=None
        )
    
        AgGrid(
            pivot_df,
            gridOptions=gb.build(),
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            allow_unsafe_jscode=True,
            theme="streamlit",
            height=400,
            fit_columns_on_grid_load=False
        )
    
        csv = pivot_df.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig")
        st.download_button("Download CSV - Fornecedores", data=csv, file_name="fornecedores.csv", mime="text/csv")

    # TABELA 2
    st.markdown("---")
    st.subheader("Quantidade Vendida por Produto por Mês")

    df_filtered_range = df[df['ANO'] >= 2024]
    anos = sorted(df_filtered_range['ANO'].unique())
    meses = sorted(df_filtered_range['MES'].unique())
    meses_nomes = [month_names[m] for m in meses]

    current_year = today.year
    current_month = today.month
    current_month_name = month_names[current_month]

    col1, col2 = st.columns(2)
    with col1:
        selected_ano = st.selectbox("Selecione o Ano", anos, index=anos.index(current_year) if current_year in anos else 0, key="ano_produto")
    with col2:
        selected_mes = st.selectbox("Selecione o Mês", meses_nomes, index=meses_nomes.index(current_month_name) if current_month_name in meses_nomes else 0, key="mes_produto")

    selected_mes_num = list(month_names.keys())[list(month_names.values()).index(selected_mes)]
    df_filtered = df_filtered_range[(df_filtered_range['MES'] == selected_mes_num) & (df_filtered_range['ANO'] == selected_ano)]

    if not df_filtered.empty:
        pivot_produtos = df_filtered.groupby(['PRODUTO', 'FORNECEDOR', 'ANO', 'MES'])['QT'].sum().reset_index()
        pivot_produtos = pivot_produtos[['PRODUTO', 'FORNECEDOR', 'QT']]

        gb_produtos = GridOptionsBuilder.from_dataframe(pivot_produtos)
        gb_produtos.configure_default_column(sortable=True, filter=True, resizable=True, minWidth=100)
        gb_produtos.configure_column("PRODUTO", pinned="left", width=250)
        gb_produtos.configure_column("FORNECEDOR", width=250)
        gb_produtos.configure_column("QT", type=["numericColumn"], valueFormatter="Math.floor(x).toLocaleString('pt-BR')", width=120)

        gb_produtos.configure_grid_options(enableRangeSelection=True)

        st.write(f"Quantidade vendida por produto para {selected_mes}-{selected_ano}:")
        AgGrid(
            pivot_produtos,
            gridOptions=gb_produtos.build(),
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            height=500,
            allow_unsafe_jscode=True,
            theme="streamlit"
        )

        csv_produtos = pivot_produtos.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig")
        st.download_button("Download CSV - Produtos", data=csv_produtos, file_name="produtos.csv", mime="text/csv")
    else:
        st.warning(f"Nenhum dado encontrado para {selected_mes}-{selected_ano}.")

if __name__ == "__main__":
    main()
