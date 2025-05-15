import streamlit as st
import pandas as pd
from datetime import datetime, date
import locale
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from supabase import create_client, Client
import time
import logging
import backoff

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurar locale para formatação monetária
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    logger.warning("Locale 'pt_BR.UTF-8' não disponível. Usando formatação manual.")

# Configuração do cliente Supabase usando secrets do Streamlit Cloud
@st.cache_resource
def init_supabase():
    try:
        SUPABASE_URL = st.secrets["SUPABASE_URL"]
        SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
    except KeyError as e:
        logger.error(f"Erro: Variável {e} não encontrada no secrets.toml.")
        return None
    
    if not SUPABASE_URL or not supposar(SUPABASE_KEY):
        logger.error("Erro: SUPABASE_URL ou SUPABASE_KEY não estão definidos.")
        return None
    
    try:
        supabase_client = create_client(SUPABASE_URL.strip(), SUPABASE_KEY.strip())
        response = supabase_client.table('VWSOMELIER').select('CODPROD').limit(1).execute()
        if not response.data:
            logger.error("Nenhum dado retornado na query de teste para VWSOMELIER.")
            return None
        return supabase_client
    except Exception as e:
        logger.error(f"Erro ao conectar ao Supabase: {e}")
        return None

supabase = init_supabase()
if supabase is None:
    logger.error("Falha ao inicializar Supabase. Encerrando.")
    st.stop()

# Função para realizar o reload automático a cada 1 minuto
def auto_reload():
    if 'last_reload' not in st.session_state:
        st.session_state.last_reload = time.time()
    
    current_time = time.time()
    if current_time - st.session_state.last_reload >= 60:  # 60 segundos
        st.session_state.last_reload = current_time
        st.cache_data.clear()  # Limpar o cache para forçar nova busca
        st.rerun()  # Forçar reload da página

# Função para obter dados do Supabase sem paginação
@st.cache_data(show_spinner=False, ttl=60)
def carregar_dados(tabela, data_inicial=None, data_final=None):
    try:
        max_retries = 3

        @backoff.on_exception(backoff.expo, Exception, max_tries=max_retries)
        def fetch_all():
            query = supabase.table(tabela).select("*")
            if data_inicial and data_final:
                if tabela == 'VWSOMELIER':
                    query = query.gte('DATA', data_inicial.isoformat()).lte('DATA', data_final.isoformat())
                elif tabela == 'PCVENDEDOR':
                    query = query.gte('DATAPEDIDO', data_inicial.isoformat()).lte('DATAPEDIDO', data_final.isoformat())
                else:
                    logger.warning(f"Tabela {tabela} não reconhecida para filtro de data.")
            return query.execute()

        response = fetch_all()
        response_data = response.data

        if not response_data:
            logger.warning(f"Nenhum dado encontrado na tabela {tabela} para o período {data_inicial} a {data_final}.")
            return pd.DataFrame()

        df = pd.DataFrame(response_data)
        df.columns = df.columns.str.strip()

        logger.info(f"Total de registros recuperados da tabela {tabela}: {len(df)}")
        return df

    except Exception as e:
        logger.error(f"Erro ao buscar dados do Supabase para tabela {tabela}: {e}")
        return pd.DataFrame()

# ... (restante do código, incluindo calcular_detalhes_vendedores, exibir_detalhes_vendedores, etc., conforme fornecido anteriormente)

def main():
    # Botão para limpar cache
    if st.button("Limpar Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    auto_reload()

    st.markdown(
        """
        <div style="display: flex; align-items: center;">
            <img src="https://cdn-icons-png.flaticon.com/512/1028/1028011.png" 
                 width="40" style="margin-right: 10px;">
            <h2 style="margin: 0;"> Detalhes Vendedores</h2>
        </div>
        """,
        unsafe_allow_html=True)

    st.markdown("### Resumo de Vendas")
    
    st.markdown(
        """
        <div style="display: flex; align-items: center;">
            <img src="https://cdn-icons-png.flaticon.com/512/6428/6428747.png" 
                 width="40" style="margin-right: 10px;">
            <p style="margin: 0;">Filtro</p>
        </div>
        """,
        unsafe_allow_html=True)
    
    data_inicial = st.date_input("Data Inicial", value=date(2024, 4, 7))
    data_final = st.date_input("Data Final", value=date(2025, 5, 14))

    if data_inicial > data_final:
        logger.error("A Data Inicial não pode ser maior que a Data Final.")
        return

    with st.spinner("Carregando dados do Supabase..."):
        data_vwsomelier = carregar_dados('VWSOMELIER', data_inicial, data_final)
        data_pcpedc = carregar_dados('PCVENDEDOR', data_inicial, data_final)
    
    if data_vwsomelier.empty or data_pcpedc.empty:
        logger.error("Não foi possível carregar os dados do Supabase.")
        return

    data_inicial = pd.to_datetime(data_inicial)
    data_final = pd.to_datetime(data_final)
    vendedores, data_filtrada = calcular_detalhes_vendedores(data_vwsomelier, data_pcpedc, data_inicial, data_final)

    if not vendedores.empty:
        exibir_detalhes_vendedores(vendedores)
        vendedores_sorted = vendedores['NOME'].str.strip().str.upper().sort_values().reset_index(drop=True)

        if 'ALTOMERCADO' in vendedores_sorted.values:
            vendedor_default = vendedores_sorted[vendedores_sorted == 'ALTOMERCADO'].index[0]
        else:
            vendedor_default = 0

        vendedor_default = int(vendedor_default)
        vendedores_display = vendedores['NOME'].str.strip().sort_values().reset_index(drop=True)
        vendedor_selecionado = st.selectbox("Selecione um Vendedor", vendedores_display, index=vendedor_default)
        ano_selecionado = st.selectbox("Selecione um Ano para o Gráfico", [2024, 2025], index=1 if datetime.now().year == 2025 else 0)
        exibir_grafico_vendas_por_vendedor(data_filtrada, vendedor_selecionado, ano_selecionado)
    else:
        logger.warning("Não há dados para o período selecionado.")

    st.markdown("---")
    st.markdown("## Detalhamento Venda Produto ##")
    st.markdown("### Filtro de Período")
    vendas_data_inicial = st.date_input("Data Inicial para Vendas", value=date(2024, 1, 1), key="vendas_inicial")
    vendas_data_final = st.date_input("Data Final para Vendas", value=date(2025, 5, 14), key="vendas_final")

    if vendas_data_inicial > vendas_data_final:
        logger.error("A Data Inicial não pode ser maior que a Data Final na seção de vendas por cliente.")
        return

    with st.spinner("Carregando dados de vendas..."):
        data_vendas = carregar_dados('PCVENDEDOR', vendas_data_inicial, vendas_data_final)

    if data_vendas.empty:
        logger.error("Dados de vendas não puderam ser carregados para o período selecionado.")
        return

    data_vendas['DATAPEDIDO'] = pd.to_datetime(data_vendas['DATAPEDIDO'], errors='coerce')
    data_vendas = data_vendas.dropna(subset=['DATAPEDIDO'])
    vendas_data_inicial = pd.to_datetime(vendas_data_inicial)
    vendas_data_final = pd.to_datetime(vendas_data_final)
    data_vendas = data_vendas[(data_vendas['DATAPEDIDO'] >= vendas_data_inicial) & 
                              (data_vendas['DATAPEDIDO'] <= vendas_data_final)].copy()

    if data_vendas.empty:
        logger.warning("Nenhum dado encontrado para o período selecionado na seção de vendas por cliente.")
        return

    opcoes_filtro = []
    if 'FORNECEDOR' in data_vendas.columns:
        opcoes_filtro.append("Fornecedor")
    if 'PRODUTO' in data_vendas.columns:
        opcoes_filtro.append("Produto")

    if not opcoes_filtro:
        logger.error("Nenhum filtro disponível.")
        st.stop()

    tipo_filtro = st.radio(
        "Filtrar por:", 
        opcoes_filtro, 
        horizontal=True,
        key="filtro_principal_radio"
    )

    col_filtros, col_bloqueado = st.columns(2)

    with col_filtros:
        if tipo_filtro == "Fornecedor":
            fornecedores = sorted(data_vendas['FORNECEDOR'].dropna().unique())
            selecionar_todos = st.checkbox(
                "Selecionar Todos os Fornecedores", 
                key="todos_fornecedores_check"
            )
            if selecionar_todos:
                itens_selecionados = fornecedores
                placeholder = "Todos os fornecedores selecionados"
            else:
                itens_selecionados = st.multiselect(
                    "Selecione os fornecedores:",
                    fornecedores,
                    key="fornecedores_multiselect"
                )
                placeholder = None
            
            if selecionar_todos:
                st.text(placeholder)
        
        elif tipo_filtro == "Produto":
            produtos = sorted(data_vendas['PRODUTO'].dropna().unique())
            selecionar_todos = st.checkbox(
                "Selecionar Todos os Produtos", 
                key="todos_produtos_check"
            )
            if selecionar_todos:
                itens_selecionados = produtos
                placeholder = "Todos os produtos selecionados"
            else:
                itens_selecionados = st.multiselect(
                    "Selecione os produtos:",
                    produtos,
                    key="produtos_multiselect"
                )
                placeholder = None
            
            if selecionar_todos:
                st.text(placeholder)
    
    with col_bloqueado:
        if 'BLOQUEADO' in data_vendas.columns:
            filtro_bloqueado = st.radio(
                "Clientes:", 
                ["Todos", "Bloqueado", "Não bloqueado"],
                horizontal=True,
                key="filtro_bloqueado_radio"
            )
        else:
            filtro_bloqueado = "Todos"

    vendedores = sorted(data_vendas['VENDEDOR'].dropna().unique())
    selecionar_todos_vendedores = st.checkbox(
        "Selecionar Todos os Vendedores", 
        key="todos_vendedores_check"
    )
    if selecionar_todos_vendedores:
        vendedores_selecionados = vendedores
        st.text("Todos os vendedores selecionados")
    else:
        vendedores_selecionados = st.multiselect(
            "Filtrar por Vendedor (opcional):",
            vendedores,
            key="vendedores_multiselect"
        )

    if st.button("Gerar Relatório", key="gerar_relatorio_btn"):
        if not itens_selecionados:
            logger.warning("Nenhum item selecionado para gerar o relatório.")
            return

        with st.spinner("Processando dados..."):
            if 'BLOQUEADO' in data_vendas.columns:
                if filtro_bloqueado == "Bloqueado":
                    data_vendas = data_vendas[data_vendas['BLOQUEADO'] == 'S'].copy()
                elif filtro_bloqueado == "Não bloqueado":
                    data_vendas = data_vendas[data_vendas['BLOQUEADO'] == 'N'].copy()
            
            if not vendedores_selecionados or len(vendedores_selecionados) == len(vendedores):
                tabela = criar_tabela_vendas_mensais(data_vendas, tipo_filtro, itens_selecionados)
                if not tabela.empty:
                    gb = GridOptionsBuilder.from_dataframe(tabela)
                    gb.configure_default_column(filter=True, sortable=True, resizable=True)
                    gb.configure_column("TOTAL", filter=False)
                    grid_options = gb.build()

                    AgGrid(
                        tabela,
                        gridOptions=grid_options,
                        update_mode=GridUpdateMode.NO_UPDATE,
                        fit_columns_on_grid_load=False,
                        height=400,
                        allow_unsafe_jscode=True,
                    )

                    csv = tabela.to_csv(index=False, sep=';', decimal=',').encode('utf-8')
                    st.download_button(
                        f"📥 Baixar CSV - {tipo_filtro}", 
                        data=csv,
                        file_name=f"vendas_{tipo_filtro.lower()}_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime='text/csv'
                    )
                else:
                    logger.warning(f"Nenhum dado encontrado para {tipo_filtro}: {', '.join(itens_selecionados)}")
            
            else:
                for vendedor in vendedores_selecionados:
                    st.markdown(f"#### Vendedor: {vendedor}")
                    tabela = criar_tabela_vendas_mensais(data_vendas, tipo_filtro, itens_selecionados, vendedor)
                    if not tabela.empty:
                        gb = GridOptionsBuilder.from_dataframe(tabela)
                        gb.configure_default_column(filter=True, sortable=True, resizable=True)
                        gb.configure_column("TOTAL", filter=False)
                        grid_options = gb.build()

                        AgGrid(
                            tabela,
                            gridOptions=grid_options,
                            update_mode=GridUpdateMode.NO_UPDATE,
                            fit_columns_on_grid_load=False,
                            height=400,
                            allow_unsafe_jscode=True,
                            wrapText=True,
                            autoHeight=True
                        )

                        csv = tabela.to_csv(index=False, sep=';', decimal=',').encode('utf-8')
                        st.download_button(
                            f"📥 Baixar CSV - {tipo_filtro} - {vendedor}", 
                            data=csv,
                            file_name=f"vendas_{tipo_filtro.lower()}_{vendedor}_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime='text/csv'
                        )
                    else:
                        logger.warning(f"Nenhum dado encontrado para {tipo_filtro}: {', '.join(itens_selecionados)} e vendedor {vendedor}")

if __name__ == "__main__":
    main()
