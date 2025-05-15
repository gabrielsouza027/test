import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from cachetools import TTLCache
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import os

# Configuração do cache
cache = TTLCache(maxsize=1, ttl=180)

# Conexão com o Supabase usando variáveis de ambiente
@st.cache_resource
def init_connection():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)

supabase: Client = init_connection()

# Buscar dados da tabela do Supabase com cache
def get_data_from_supabase(data_inicial, data_final):
    key = f"{data_inicial.strftime('%Y-%m-%d')}_{data_final.strftime('%Y-%m-%d')}"
    if key not in cache:
        try:
            response = (
                supabase.table("vendas")  # nome da tabela no Supabase
                .select("*")
                .gte("DATA", data_inicial.strftime("%Y-%m-%d"))
                .lte("DATA", data_final.strftime("%Y-%m-%d"))
                .limit(999999)
                .execute()
            )
            data = response.data
            df = pd.DataFrame(data)

            required_columns = ['DATA', 'QT', 'PVENDA', 'FORNECEDOR', 'VENDEDOR', 'CLIENTE', 'PRODUTO', 'CODPROD', 'CODIGOVENDEDOR', 'CODCLI']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Coluna '{col}' não encontrada")

            df['DATA'] = pd.to_datetime(df['DATA'])
            df['MES'] = df['DATA'].dt.month
            df['ANO'] = df['DATA'].dt.year
            df['VALOR_TOTAL_ITEM'] = df['QT'] * df['PVENDA']

            cache[key] = df
        except Exception as e:
            st.error(f"Erro ao buscar dados do Supabase: {e}")
            cache[key] = pd.DataFrame()
    return cache[key]

def main():
    st.title("📊 Dashboard de Vendas (Supabase + Streamlit Cloud)")

    st.subheader("Filtro de Período (Fornecedores)")
    today = datetime.today()
    col1, col2 = st.columns(2)
    with col1:
        data_inicial = st.date_input("Data Inicial", value=datetime(today.year - 1, 1, 1), key="data_inicial")
    with col2:
        data_final = st.date_input("Data Final", value=today, key="data_final")

    data_inicial = datetime.combine(data_inicial, datetime.min.time())
    data_final = datetime.combine(data_final, datetime.max.time())

    if data_inicial > data_final:
        st.error("A data inicial não pode ser maior que a data final.")
        return

    df = get_data_from_supabase(data_inicial, data_final)

    if df.empty:
        st.warning("Nenhum dado encontrado para o período selecionado.")
        return

    # -------------------- TABELA 1 --------------------
    st.subheader("Valor Total por Fornecedor por Mês")
    search_term = st.text_input("Pesquisar Fornecedor:", "", key="search_fornecedor")

    df_grouped = df.groupby(['FORNECEDOR', 'MES', 'ANO'])['VALOR_TOTAL_ITEM'].sum().reset_index()

    pivot_df = df_grouped.pivot_table(
        values='VALOR_TOTAL_ITEM',
        index='FORNECEDOR',
        columns=['ANO', 'MES'],
        aggfunc='sum',
        fill_value=0
    )

    month_names = {
        1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }
    pivot_df.columns = [f"{month_names[mes]}-{ano}" for ano, mes in pivot_df.columns]
    pivot_df['Total'] = pivot_df.sum(axis=1)
    pivot_df = pivot_df.reset_index()

    if search_term:
        pivot_df = pivot_df[pivot_df['FORNECEDOR'].str.contains(search_term, case=False, na=False)]

    if pivot_df.empty:
        st.warning("Nenhum fornecedor encontrado com o termo pesquisado.")
    else:
        gb = GridOptionsBuilder.from_dataframe(pivot_df)
        gb.configure_default_column(sortable=True, filter=True, resizable=True, minWidth=100)
        gb.configure_column("FORNECEDOR", headerName="Fornecedor", pinned="left", width=300)

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

        gb.configure_grid_options(enableRangeSelection=True)
        AgGrid(
            pivot_df,
            gridOptions=gb.build(),
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            height=400,
            allow_unsafe_jscode=True,
            theme="streamlit"
        )

        csv = pivot_df.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig")
        st.download_button("Download CSV - Fornecedores", data=csv, file_name="fornecedores.csv", mime="text/csv")

    # -------------------- TABELA 2 --------------------
    st.markdown("---")
    st.subheader("Quantidade Vendida por Produto por Mês")

    anos = sorted(df['ANO'].unique())
    meses = sorted(df['MES'].unique())
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
    df_filtered = df[(df['MES'] == selected_mes_num) & (df['ANO'] == selected_ano)]

    if not df_filtered.empty:
        pivot_produtos = df_filtered.groupby(
            ['CODPROD', 'PRODUTO', 'CODIGOVENDEDOR', 'VENDEDOR', 'CODCLI', 'CLIENTE', 'FORNECEDOR']
        )['QT'].sum().reset_index()

        pivot_produtos = pivot_produtos[['PRODUTO', 'CODPROD', 'VENDEDOR', 'CODIGOVENDEDOR', 'CLIENTE', 'CODCLI', 'FORNECEDOR', 'QT']]

        gb_produtos = GridOptionsBuilder.from_dataframe(pivot_produtos)
        gb_produtos.configure_default_column(sortable=True, filter=True, resizable=True, minWidth=100)
        gb_produtos.configure_column("PRODUTO", pinned="left", width=250)
        gb_produtos.configure_column("CODPROD", width=120)
        gb_produtos.configure_column("VENDEDOR", width=250)
        gb_produtos.configure_column("CODIGOVENDEDOR", width=120)
        gb_produtos.configure_column("CLIENTE", width=250)
        gb_produtos.configure_column("CODCLI", width=120)
        gb_produtos.configure_column("FORNECEDOR", width=250)
        gb_produtos.configure_column("QT", type=["numericColumn"], valueFormatter="Math.floor(x).toLocaleString('pt-BR')", width=120)

        gb_produtos.configure_grid_options(enableRangeSelection=True)

        st.write(f"Quantidade vendida por produto para {selected_mes}-{selected_ano}:")
        AgGrid(
            pivot_produtos,
            gridOptions=gb_produtos.build(),
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            height=400,
            allow_unsafe_jscode=True,
            theme="streamlit"
        )

        csv_produtos = pivot_produtos.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig")
        st.download_button("Download CSV - Produtos", data=csv_produtos, file_name="produtos.csv", mime="text/csv")
    else:
        st.warning("Nenhum dado encontrado para o mês e ano selecionados.")

if __name__ == "__main__":
    main()
