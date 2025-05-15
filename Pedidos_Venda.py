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

# Configuração do cache (TTL de 60 segundos)
cache = TTLCache(maxsize=10, ttl=60)

# Configuração das tabelas e colunas esperadas
SUPABASE_CONFIG = {
    "pedidos": {
        "table": "PCPEDI",
        "columns": ['created_at', 'NUMPED', 'NUMCAR', 'DATA', 'CODCLI', 'QT', 'CODPROD', 'PVENDA', 
                   'POSICAO', 'CLIENTE', 'DESCRICAO_PRODUTO', 'CODIGO_VENDEDOR', 'NOME_VENDEDOR', 'NUMNOTA', 
                   'OBS', 'OBS1', 'OBS2', 'CODFILIAL', 'MUNICIPIO']
    }
    # Adicione mais tabelas aqui, se necessário
    # "outra_tabela": {
    #     "table": "NOME_DA_TABELA",
    #     "columns": ["COLUNA1", "COLUNA2", ...]
    # }
}

# Função para buscar dados do Supabase com paginação
@st.cache_data(show_spinner=False, ttl=60)
def fetch_pedidos(_cache, table, columns_expected, data_inicial, data_final):
    key = f"{table}_{data_inicial}_{data_final}"
    if key in _cache:
        logger.info(f"Dados da tabela {table} recuperados do cache")
        return _cache[key]

    try:
        # Formatar datas para compatibilidade com coluna 'DATA'
        data_inicial_str = data_inicial.strftime("%Y-%m-%d")
        data_final_str = data_final.strftime("%Y-%m-%d")
        
        all_data = []
        offset = 0
        limit = 1000  # Ajustado para um limite menor e mais seguro

        while True:
            response = supabase.table(table).select("*").gte("DATA", data_inicial_str).lte("DATA", data_final_str).range(offset, offset + limit - 1).execute()
            data = response.data
            if not data:
                logger.info(f"Finalizada a recuperação de dados da tabela {table}")
                break
            all_data.extend(data)
            offset += limit
            logger.info(f"Recuperados {len(data)} registros da tabela {table}, total até agora: {len(all_data)}")

        if not all_data:
            logger.warning(f"Nenhum dado encontrado entre {data_inicial} e {data_final}")
            st.warning(f"Nenhum dado encontrado entre {data_inicial} e {data_final}.")
            _cache[key] = pd.DataFrame()
            return _cache[key]
        
        df = pd.DataFrame(all_data)
        
        # Verificar colunas obrigatórias
        missing_columns = [col for col in columns_expected if col not in df.columns]
        if missing_columns:
            logger.error(f"Colunas obrigatórias não encontradas: {', '.join(missing_columns)}")
            st.error(f"Colunas obrigatórias não encontradas: {', '.join(missing_columns)}")
            _cache[key] = pd.DataFrame()
            return _cache[key]
        
        # Converter tipos
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
        df['QT'] = pd.to_numeric(df['QT'], errors='coerce').fillna(0)
        df['PVENDA'] = pd.to_numeric(df['PVENDA'], errors='coerce').fillna(0)
        df['NUMPED'] = pd.to_numeric(df['NUMPED'], errors='coerce').fillna(0)
        df['NUMCAR'] = pd.to_numeric(df['NUMCAR'], errors='coerce').fillna(0)
        df['CODCLI'] = pd.to_numeric(df['CODCLI'], errors='coerce').fillna(0)
        df['CODPROD'] = pd.to_numeric(df['CODPROD'], errors='coerce').fillna(0)

        # Calcular valor total por pedido
        df['valor_total'] = df['QT'] * df['PVENDA']
        
        _cache[key] = df
        logger.info(f"Dados carregados com sucesso da tabela {table}: {len(df)} registros")
    except Exception as e:
        logger.error(f"Erro ao buscar dados do Supabase: {e}")
        st.error(f"Erro ao buscar dados do Supabase: {e}")
        _cache[key] = pd.DataFrame()
    return _cache[key]

# Função para mapear os valores de POSICAO e adicionar cor
def formatar_posicao(posicao):
    posicao_map = {
        'L': ('LIBERADO', '#008000'), 
        'M': ('MONTADO', '#FFA500'), 
        'F': ('FATURADO', '#0000FF'), 
        'C': ('CANCELADO', '#FF0000')
    }
    texto, cor = posicao_map.get(posicao, (posicao, '#000000'))
    return f'<span style="color:{cor}">{texto}</span>'

# Função para realizar o reload automático a cada 1 minuto
def auto_reload():
    if 'last_reload' not in st.session_state:
        st.session_state.last_reload = time.time()
    
    current_time = time.time()
    if current_time - st.session_state.last_reload >= 60:  # 60 segundos
        st.session_state.last_reload = current_time
        st.cache_data.clear()  # Limpar o cache para forçar nova busca
        st.rerun()  # Forçar reload da página

# Função principal do Streamlit
def main():
    st.title("Pedidos de Venda")

    # Chamar auto_reload para verificar se precisa atualizar
    auto_reload()

    # Inicializar session_state
    if 'pedidos_list' not in st.session_state:
        st.session_state.pedidos_list = []
    if 'display_limit' not in st.session_state:
        st.session_state.display_limit = 50
    if 'selected_filiais' not in st.session_state:
        st.session_state.selected_filiais = []
    if 'selected_rotas' not in st.session_state:
        st.session_state.selected_rotas = []

    # Seção de Filtros
    with st.container(border=True):
        st.markdown("### Filtros", unsafe_allow_html=True)

        # Período (ajustado para a data atual: 09:10 PM -03, 13/05/2025)
        st.markdown("**📅 Período**", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            data_inicial = st.date_input("Data Inicial", date(2025, 5, 13), key="data_inicial")
        with col2:
            data_final = st.date_input("Data Final", date(2025, 5, 13), key="data_final")
        st.divider()

        if data_inicial > data_final:
            st.error("A data inicial não pode ser maior que a data final.")
            return

        # Pesquisa
        st.markdown("**🔍 Pesquisa**", unsafe_allow_html=True)
        col3, col4 = st.columns(2)
        with col3:
            search_client = st.text_input("Cliente ou Pedido", "", placeholder="Código, nome ou nº do pedido")
        with col4:
            search_seller = st.text_input("Vendedor", "", placeholder="Código ou nome do vendedor")
        st.divider()

    # Carregar os dados
    config = SUPABASE_CONFIG["pedidos"]
    with st.spinner(f"Carregando pedidos entre {data_inicial} e {data_final}..."):
        df_pedidos = fetch_pedidos(cache, config["table"], config["columns"], data_inicial, data_final)

    if df_pedidos.empty:
        st.warning("Nenhum pedido encontrado ou erro ao carregar os dados.")
        return
        
    df_pedidos['valor_item'] = df_pedidos['QT'] * df_pedidos['PVENDA']


    # Processar dados (agrupar por NUMPED)
    df_grouped = df_pedidos.groupby('NUMPED').agg({
        'created_at': 'first', 'NUMCAR': 'first', 'DATA': 'first', 'CODCLI': 'first', 'CLIENTE': 'first',
        'CODIGO_VENDEDOR': 'first', 'NOME_VENDEDOR': 'first', 'NUMNOTA': 'first', 'OBS': 'first',
        'OBS1': 'first', 'OBS2': 'first', 'POSICAO': 'first', 'CODFILIAL': 'first',
        'MUNICIPIO': 'first', 'valor_item': 'sum'
    }).reset_index()

    df_grouped = df_grouped.rename(columns={'valor_item': 'valor_total'})
    pedidos_dict = df_grouped.to_dict('records')
    pedidos_list_full = pedidos_dict
    filiais_unicas = sorted(set(df_grouped['CODFILIAL'].dropna().astype(str)))

    # Filtros Avançados e Status
    with st.container(border=True):
        # Filtros Avançados - Filiais
        st.markdown("**🏢 Filiais**", unsafe_allow_html=True)
        col5, col6 = st.columns(2)
        with col5:
            for filial in filiais_unicas[:len(filiais_unicas)//2 + 1]:
                if st.checkbox(f"Filial {filial}", value=filial in st.session_state.selected_filiais, key=f"filial_{filial}"):
                    if filial not in st.session_state.selected_filiais:
                        st.session_state.selected_filiais.append(filial)
                else:
                    if filial in st.session_state.selected_filiais:
                        st.session_state.selected_filiais.remove(filial)
        with col6:
            for filial in filiais_unicas[len(filiais_unicas)//2 + 1:]:
                if st.checkbox(f"Filial {filial}", value=filial in st.session_state.selected_filiais, key=f"filial_{filial}"):
                    if filial not in st.session_state.selected_filiais:
                        st.session_state.selected_filiais.append(filial)
                else:
                    if filial in st.session_state.selected_filiais:
                        st.session_state.selected_filiais.remove(filial)
        if st.button("Selecionar Todas as Filiais", key="select_all_filial", use_container_width=True):
            st.session_state.selected_filiais = filiais_unicas.copy()
        st.divider()

        # Status
        st.markdown("**📊 Status**", unsafe_allow_html=True)
        col9, col10, col11, col12 = st.columns(4)
        with col9:
            show_liberado = st.checkbox("✅ Liberado", value=True, key="liberado")
        with col10:
            show_montado = st.checkbox("📦 Montado", value=True, key="montado")
        with col11:
            show_faturado = st.checkbox("💳 Faturado", value=True, key="faturado")
        with col12:
            show_cancelado = st.checkbox("❌ Cancelado", value=False, key="cancelado")
        st.divider()

        # Botão Aplicar Filtros
        if st.button("Aplicar Filtros", key="apply_filters", type="primary", use_container_width=True):
            pedidos_list = pedidos_list_full
            if search_client:
                pedidos_list = [p for p in pedidos_list if search_client.lower() in str(p.get('CODCLI', '')).lower() or 
                                search_client.lower() in str(p.get('CLIENTE', '')).lower() or 
                                search_client.lower() in str(p.get('NUMPED', '')).lower()]
            if search_seller:
                pedidos_list = [p for p in pedidos_list if search_seller.lower() in str(p.get('NOME_VENDEDOR', '')).lower() or
                                search_seller.lower() in str(p.get('CODIGO_VENDEDOR', '')).lower()]

            if st.session_state.selected_filiais:
                pedidos_list = [p for p in pedidos_list if str(p.get('CODFILIAL', '')) in st.session_state.selected_filiais]
            if not (show_liberado and show_montado and show_faturado and show_cancelado):
                selected_positions = []
                if show_liberado: selected_positions.append('L')
                if show_montado: selected_positions.append('M')
                if show_faturado: selected_positions.append('F')
                if show_cancelado: selected_positions.append('C')
                if selected_positions:
                    pedidos_list = [p for p in pedidos_list if str(p.get('POSICAO', '')) in selected_positions]
            if not pedidos_list:
                st.warning("Nenhum pedido encontrado com os critérios de pesquisa.")
                st.session_state.pedidos_list = []
            else:
                st.session_state.pedidos_list = pedidos_list
            st.session_state.display_limit = 50

    # Exibir pedidos
    if st.session_state.pedidos_list:
        st.header("Lista de Pedidos", divider="gray")
        st.write(f"Total de pedidos exibidos: {len(st.session_state.pedidos_list)} (Mostrando até {st.session_state.display_limit} de {len(st.session_state.pedidos_list)})")
        
        for pedido in st.session_state.pedidos_list[:st.session_state.display_limit]:
            with st.expander(f"Pedido {pedido.get('NUMPED', 'N/A')} - Cliente: {pedido.get('CLIENTE', 'N/A')} ({pedido.get('MUNICIPIO', 'N/A')})"):
                col5, col6 = st.columns(2)
                with col5:
                    st.markdown(f"""
                        **Nº Pedido:** {pedido.get('NUMPED', 'N/A')}  
                        **Nº Carregamento:** {pedido.get('NUMCAR', 'N/A')}  
                        **Data:** {pedido.get('DATA', 'N/A').strftime('%Y-%m-%d') if pd.notna(pedido.get('DATA')) else 'N/A'}  
                        **Cód. Cliente:** {pedido.get('CODCLI', 'N/A')}  
                        **Cliente:** {pedido.get('CLIENTE', 'N/A')}  
                        **Cidade:** {pedido.get('MUNICIPIO', 'N/A')}  
                        **Posição:** {formatar_posicao(pedido.get('POSICAO', ''))}  
                    """, unsafe_allow_html=True)
                with col6:
                    st.markdown(f"""
                        **Cód. Vendedor:** {pedido.get('CODIGO_VENDEDOR', 'N/A')}  
                        **Vendedor:** {pedido.get('NOME_VENDEDOR', 'N/A')}  
                        **Nº Nota:** {pedido.get('NUMNOTA', 'N/A')}  
                        **Cód. Filial:** {pedido.get('CODFILIAL', 'N/A')}  
                        **Observação:** {pedido.get('OBS', 'N/A')}  
                        **Observação 1:** {pedido.get('OBS1', 'N/A')}  
                        **Observação 2:** {pedido.get('OBS2', 'N/A')}  
                        **Valor Total:** R$ {pedido.get('valor_total', 0):,.2f}
                    """, unsafe_allow_html=True)
                st.subheader("Produtos")
                produtos_df = df_pedidos[df_pedidos['NUMPED'] == pedido.get('NUMPED', '')][['CODPROD', 'DESCRICAO_PRODUTO', 'QT', 'PVENDA', 'POSICAO']]
                produtos_df["VALOR_TOTAL_ITEM"] = produtos_df["QT"] * produtos_df["PVENDA"]
                produtos_df = produtos_df.rename(columns={
                    "CODPROD": "Código Produto", "DESCRICAO": "Descrição", "QT": "Quantidade",
                    "PVENDA": "Preço Unitário", "VALOR_TOTAL_ITEM": "Valor Total", "POSICAO": "Posição"
                })
                if not produtos_df.empty:
                    styled_df = produtos_df.style.format({
                        "Preço Unitário": "R$ {:.2f}", "Valor Total": "R$ {:.2f}", "Quantidade": "{:.0f}"
                    }).set_properties(**{
                        'text-align': 'center', 'font-size': '12pt', 'border': '1px solid #ddd', 'padding': '5px'
                    }).set_table_styles([
                        {'selector': 'th', 'props': [('background-color', '#f4f4f4'), ('font-weight', 'bold'),
                                                    ('text-align', 'center'), ('border', '1px solid #ddd'), ('padding', '5px')]}
                    ]).hide(axis="index")
                    st.dataframe(styled_df, height=300, use_container_width=True)
                else:
                    st.info("Nenhum produto encontrado para este pedido.")

        if st.session_state.display_limit < len(st.session_state.pedidos_list):
            st.button("Carregar Mais", key="load_more", on_click=lambda: st.session_state.update(display_limit=st.session_state.display_limit + 50))
    else:
        st.info("Nenhum pedido disponível para exibição. Aplique os filtros para carregar os dados.")

if __name__ == "__main__":
    main()
