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
    
    if not SUPABASE_URL or not SUPABASE_KEY:
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

def calcular_detalhes_vendedores(data_vwsomelier, data_pcpedc, data_inicial, data_final):
    required_columns_vwsomelier = ['DATA', 'PVENDA', 'QT', 'NUMPED', 'CODPROD']
    required_columns_pcpedc = ['CODUSUR', 'VENDEDOR', 'CODCLIENTE', 'PEDIDO']
    
    missing_columns_vwsomelier = [col for col in required_columns_vwsomelier if col not in data_vwsomelier.columns]
    missing_columns_pcpedc = [col for col in required_columns_pcpedc if col not in data_pcpedc.columns]
    
    if missing_columns_vwsomelier or missing_columns_pcpedc or data_vwsomelier.empty or data_pcpedc.empty:
        logger.error(f"Colunas faltando em VWSOMELIER: {missing_columns_vwsomelier}, PCVENDEDOR: {missing_columns_pcpedc}")
        return pd.DataFrame(), pd.DataFrame()

    try:
        data_vwsomelier['DATA'] = pd.to_datetime(data_vwsomelier['DATA'], errors='coerce')
        data_vwsomelier['PVENDA'] = pd.to_numeric(data_vwsomelier['PVENDA'], errors='coerce').fillna(0).astype('float32')
        data_vwsomelier['QT'] = pd.to_numeric(data_vwsomelier['QT'], errors='coerce').fillna(0).astype('int32')
        data_vwsomelier['NUMPED'] = data_vwsomelier['NUMPED'].astype(str).str.strip()
        data_pcpedc['CODUSUR'] = data_pcpedc['CODUSUR'].astype(str).str.strip()
        data_pcpedc['CODCLIENTE'] = data_pcpedc['CODCLIENTE'].astype(str).str.strip()
        data_pcpedc['PEDIDO'] = data_pcpedc['PEDIDO'].astype(str).str.strip()
    except Exception as e:
        logger.error(f"Erro ao converter tipos de dados: {e}")
        return pd.DataFrame(), pd.DataFrame()

    data_vwsomelier['NUMPED'] = data_vwsomelier['NUMPED'].str.replace(r'\.0$', '', regex=True)
    data_pcpedc['PEDIDO'] = data_pcpedc['PEDIDO'].str.replace(r'\.0$', '', regex=True)

    data_vwsomelier = data_vwsomelier[data_vwsomelier['NUMPED'].notna() & (data_vwsomelier['NUMPED'] != '')]
    data_pcpedc = data_pcpedc[data_pcpedc['PEDIDO'].notna() & (data_pcpedc['PEDIDO'] != '')]

    data_filtrada = data_vwsomelier[(data_vwsomelier['DATA'] >= data_inicial) & 
                                    (data_vwsomelier['DATA'] <= data_final)].copy()

    if data_filtrada.empty:
        logger.warning("Não há dados para o período selecionado em VWSOMELIER.")
        return pd.DataFrame(), pd.DataFrame()

    if 'DTCANCEL' in data_filtrada.columns:
        data_filtrada = data_filtrada[data_filtrada['DTCANCEL'].isna()]

    if data_filtrada['NUMPED'].isna().all() or data_pcpedc['PEDIDO'].isna().all():
        logger.warning("Nenhum valor válido em NUMPED ou PEDIDO para realizar a junção.")
        return pd.DataFrame(), pd.DataFrame()

    logger.info(f"Tipos de dados em data_vwsomelier: {data_vwsomelier[['NUMPED']].dtypes}")
    logger.info(f"Tipos de dados em data_pcpedc: {data_pcpedc[['PEDIDO']].dtypes}")
    logger.info(f"Amostra de NUMPED: {data_vwsomelier['NUMPED'].head().tolist()}")
    logger.info(f"Amostra de PEDIDO: {data_pcpedc['PEDIDO'].head().tolist()}")

    data_filtrada = data_filtrada.merge(
        data_pcpedc[['PEDIDO', 'CODUSUR', 'VENDEDOR', 'CODCLIENTE']],
        left_on='NUMPED',
        right_on='PEDIDO',
        how='left'
    )
    logger.info(f"Tamanho de data_filtrada após merge: {len(data_filtrada)}")

    if data_filtrada.empty:
        logger.warning("Nenhum dado correspondente encontrado ao combinar VWSOMELIER e PCVENDEDOR.")
        return pd.DataFrame(), pd.DataFrame()

    data_filtrada = data_filtrada[data_filtrada['PVENDA'].notna() & data_filtrada['QT'].notna()]
    logger.info(f"Linhas após remover NaN em PVENDA/QT: {len(data_filtrada)}")

    data_filtrada['TOTAL_VENDAS'] = data_filtrada['PVENDA'] * data_filtrada['QT']

    vendedores = data_filtrada.groupby('CODUSUR').agg(
        vendedor=('VENDEDOR', 'first'),
        total_vendas=('TOTAL_VENDAS', 'sum'),
        total_clientes=('CODCLIENTE', 'nunique'),
        total_pedidos=('NUMPED', 'nunique'),
    ).reset_index()

    vendedores['total_vendas'] = pd.to_numeric(vendedores['total_vendas'], errors='coerce').fillna(0)
    logger.info(f"Valores em total_vendas após limpeza: {vendedores['total_vendas'].head().tolist()}")

    vendedores.rename(columns={
        'CODUSUR': 'RCA',
        'vendedor': 'NOME',
        'total_vendas': 'TOTAL VENDAS',
        'total_clientes': 'TOTAL CLIENTES',
        'total_pedidos': 'TOTAL PEDIDOS'
    }, inplace=True)

    return vendedores, data_filtrada

def exibir_detalhes_vendedores(vendedores):
    if vendedores.empty:
        logger.warning("Nenhum dado de vendedores para exibir.")
        return

    st.markdown(
        """
        <div style="display: flex; align-items: center;">
            <img src="https://cdn-icons-png.flaticon.com/512/6633/6633057.png" 
                 width="40" style="margin-right: 10px;">
            <p style="margin: 0;">Vendedores</p>
        </div>
        """,
        unsafe_allow_html=True)

    logger.info(f"Tipos de dados em vendedores: {vendedores.dtypes}")
    logger.info(f"Amostra de TOTAL VENDAS: {vendedores['TOTAL VENDAS'].head().tolist()}")

    st.dataframe(vendedores.style.format({
        'TOTAL VENDAS': formatar_valor,
    }), use_container_width=True)

def formatar_valor(valor):
    try:
        if pd.isna(valor) or valor is None:
            return "R$ 0,00"
        valor = float(valor)
        return locale.currency(valor, grouping=True, symbol=True)
    except (ValueError, TypeError, locale.Error):
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def exibir_grafico_vendas_por_vendedor(data, vendedor_selecionado, ano_selecionado):
    dados_vendedor = data[
        (data['VENDEDOR'] == vendedor_selecionado) & 
        (data['DATA'].dt.year == ano_selecionado)
    ].copy()

    if dados_vendedor.empty:
        logger.warning(f"Nenhum dado encontrado para o vendedor {vendedor_selecionado} no ano {ano_selecionado}.")
        return

    meses = [f"{ano_selecionado}-{str(m).zfill(2)}" for m in range(1, 13)]
    vendas_mensais = pd.DataFrame({'MÊS': meses})

    dados_vendedor['TOTAL_VENDAS'] = dados_vendedor['PVENDA'] * dados_vendedor['QT']

    vendas_por_mes = dados_vendedor.groupby(dados_vendedor['DATA'].dt.strftime('%Y-%m')).agg(
        total_vendas=('TOTAL_VENDAS', 'sum'),
        total_clientes=('CODCLIENTE', 'nunique'),
        total_pedidos=('NUMPED', 'nunique'),
    ).reset_index().rename(columns={'DATA': 'MÊS'})

    vendas massive, vendas_mensais = vendas_mensais.merge(vendas_por_mes, on='MÊS', how='left').fillna({
        'total_vendas': 0,
        'total_clientes': 0,
        'total_pedidos': 0
    })

    vendas_mensais.rename(columns={
        'total_vendas': 'TOTAL VENDIDO',
        'total_clientes': 'TOTAL CLIENTES',
        'total_pedidos': 'TOTAL PEDIDOS',
    }, inplace=True)

    fig = px.bar(
        vendas_mensais, 
        x='TOTAL VENDIDO', 
        y='MÊS', 
        orientation='h', 
        title=f'Vendas Mensais de {vendedor_selecionado} ({ano_selecionado})',
        color='MÊS', 
        color_discrete_sequence=px.colors.qualitative.Plotly,
        hover_data={'TOTAL CLIENTES': True, 'TOTAL PEDIDOS': True, 'TOTAL VENDIDO': ':,.2f'}
    )

    fig.update_layout(
        xaxis_title="Total Vendido (R$)",
        yaxis_title="Mês",
        title_font_size=20,
        xaxis_title_font_size=16,
        yaxis_title_font_size=16,
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14,
        yaxis={'autorange': 'reversed'},
        showlegend=True
    )

    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.write("TOTAL DE CLIENTES 🧍‍♂️:", int(vendas_mensais['TOTAL CLIENTES'].sum()))
    with col2:
        st.write("TOTAL DE PEDIDOS 🚚:", int(vendas_mensais['TOTAL PEDIDOS'].sum()))

def criar_tabela_vendas_mensais(data, tipo_filtro, valores_filtro, vendedor=None):
    try:
        if data.columns.duplicated().any():
            data = data.loc[:, ~data.columns.duplicated()]

        obrigatorias = ['DATAPEDIDO', 'CODCLIENTE', 'CLIENTE', 'QUANTIDADE']
        faltantes = [col for col in obrigatorias if col not in data.columns]
        if faltantes:
            logger.error(f"Colunas obrigatórias faltando: {', '.join(faltantes)}")
            return pd.DataFrame()
        
        data['DATAPEDIDO'] = pd.to_datetime(data['DATAPEDIDO'], errors='coerce')
        data = data.dropna(subset=['DATAPEDIDO'])
        data['MES_ANO'] = data['DATAPEDIDO'].dt.to_period('M').astype(str)

        if vendedor and 'VENDEDOR' in data.columns:
            data = data[data['VENDEDOR'] == vendedor].copy()
            if data.empty:
                return pd.DataFrame()

        if tipo_filtro == "Fornecedor":
            if 'FORNECEDOR' not in data.columns:
                logger.error("A coluna 'FORNECEDOR' não está presente nos dados filtrados.")
                return pd.DataFrame()
            data = data[data['FORNECEDOR'].isin(valores_filtro)].copy()
        elif tipo_filtro == "Produto":
            if 'PRODUTO' not in data.columns:
                logger.error("A coluna 'PRODUTO' não está presente nos dados filtrados.")
                return pd.DataFrame()
            data = data[data['PRODUTO'].isin(valores_filtro)].copy()

        if data.empty:
            logger.warning(f"Nenhum dado encontrado para {tipo_filtro}: {', '.join(valores_filtro)}")
            return pd.DataFrame()

        group_cols = ['CODUSUR', 'VENDEDOR', 'ROTA', 'CODCLIENTE', 'CLIENTE']
        if 'FANTASIA' in data.columns:
            group_cols.append('FANTASIA')

        tabela = data.groupby(group_cols + ['MES_ANO'])['QUANTIDADE'].sum().unstack(fill_value=0).reset_index()
        tabela['CODCLIENTE'] = tabela['CODCLIENTE'].astype(str)
        meses = sorted([col for col in tabela.columns if col not in group_cols])
        tabela['TOTAL'] = tabela[meses].sum(axis=1)

        return tabela[group_cols + meses + ['TOTAL']]
    
    except Exception as e:
        logger.error(f"Erro ao processar dados: {str(e)}")
        return pd.DataFrame()

def criar_tabela_vendas_mensais_por/produto(data, fornecedor, ano):
    data_filtrada = data[(data['FORNECEDOR'] == fornecedor) & (data['DATAPEDIDO'].dt.year == ano)].copy()

    if data_filtrada.empty:
        return pd.DataFrame()
    
    data_filtrada['MES'] = data_filtrada['DATAPEDIDO'].dt.strftime('%b')
    tabela = pd.pivot_table(
        data_filtrada,
        values='QUANTIDADE',
        index='PRODUTO',
        columns='MES',
        aggfunc='sum',
        fill_value=0
    )

    mes_ordenado = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    tabela = tabela.reindex(columns=[m for m in mes_ordenado if m in tabela.columns])
    tabela['TOTAL'] = tabela.sum(axis=1)
    tabela = tabela.reset_index()

    return tabela

def main():

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
    data_inicial = st.date_input("Data Inicial", value=date(2024,1, 1))
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
