import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import calendar
from supabase import create_client, Client
import time
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    supabase: Client = create_client(SUPABASE_URL.strip(), SUPABASE_KEY.strip())
    # Testar conexão com uma query simples
    response = supabase.table('VWSOMELIER').select('CODPROD').limit(1).execute()
except Exception as e:
    st.error(f"Erro ao conectar ao Supabase: {e}")
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

# Função para carregar dados do Supabase com cache e paginação
@st.cache_data(show_spinner=False, ttl=60)
def carregar_dados(data_inicial="2024-01-01", data_final="2025-12-31"):
    try:
        all_data = []
        offset = 0
        limit = 1000  # Limite ajustado para evitar sobrecarga

        while True:
            response = supabase.table("VWSOMELIER").select("*").gte("DATA", data_inicial).lte("DATA", data_final).range(offset, offset + limit - 1).execute()
            response_data = response.data
            if not response_data:
                logger.info(f"Finalizada a recuperação de dados da tabela VWSOMELIER")
                break
            all_data.extend(response_data)
            offset += limit
            logger.info(f"Recuperados {len(response_data)} registros da tabela VWSOMELIER, total até agora: {len(all_data)}")

        if not all_data:
            logger.warning(f"Nenhum dado encontrado na tabela VWSOMELIER para o período {data_inicial} a {data_final}")
            st.warning(f"Nenhum dado encontrado na tabela VWSOMELIER para o período {data_inicial} a {data_final}.")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        expected_columns = ['DESCRICAO_1', 'CODPROD', 'DATA', 'QT', 'PVENDA', 'VLCUSTOFIN', 'CODOPER']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"As seguintes colunas estão faltando: {', '.join(missing_columns)}")
            st.error(f"As seguintes colunas estão faltando: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # Filtrar apenas pedidos com CODOPER = 'S' (excluir devoluções com CODOPER = 'ED')
        df = df[df['CODOPER'] == 'S'].copy()
        
        df['DESCRICAO_1'] = df['DESCRICAO_1'].fillna('').astype(str).str.strip()
        df['CÓDIGO PRODUTO'] = df['CODPROD'].fillna('').astype(str).str.strip()
        df['Data do Pedido'] = pd.to_datetime(df['DATA'], errors='coerce')

        if df['Data do Pedido'].isnull().any():
            st.warning("Existem valores inválidos ou ausentes na coluna 'DATA'.")
            invalid_dates = df[df['Data do Pedido'].isnull()]['DATA'].unique()
            st.write("Valores inválidos encontrados em 'DATA':", invalid_dates)
            df = df.dropna(subset=['Data do Pedido'])

        df['VALOR TOTAL VENDIDO'] = pd.to_numeric(df['PVENDA'], errors='coerce').fillna(0).astype('float32')
        df['VLCUSTOFIN'] = pd.to_numeric(df['VLCUSTOFIN'], errors='coerce').fillna(0).astype('float32')
        df['QT'] = pd.to_numeric(df['QT'], errors='coerce').fillna(0).astype('int32')
        df['Margem de Lucro'] = (df['VALOR TOTAL VENDIDO'] - df['VLCUSTOFIN']).astype('float32')
        df['Ano'] = df['Data do Pedido'].dt.year
        df['Mês'] = df['Data do Pedido'].dt.month
        
        return df
    except Exception as e:
        logger.error(f"Erro ao consultar o Supabase: {e}")
        st.error(f"Erro ao consultar o Supabase: {e}")
        return pd.DataFrame()

# Função para formatar valores monetários
def formatar_valor(valor):
    try:
        return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except:
        return f"R$ {valor:.2f}"

# Função para formatar quantidades
def formatar_quantidade(valor):
    try:
        return f"{valor:,.0f}".replace(',', '.')
    except:
        return f"{valor:.0f}"

# Funções de exibição
def exibir_tabela(df_filtrado):
    if df_filtrado.empty:
        st.warning("Nenhum dado disponível para exibir na tabela.")
        return
    
    df_resumo = df_filtrado.groupby(['CÓDIGO PRODUTO', 'DESCRICAO_1']).agg(
        Total_Vendido=('QT', 'sum'),
        Valor_Total_Vendido=('VALOR TOTAL VENDIDO', 'sum')
    ).reset_index()

    df_resumo.rename(columns={
        'Valor_Total_Vendido': 'VALOR TOTAL VENDIDO',
        'Total_Vendido': 'QUANTIDADE'
    }, inplace=True)

    df_resumo['VALOR TOTAL VENDIDO'] = df_resumo['VALOR TOTAL VENDIDO'].apply(formatar_valor)
    df_resumo['QUANTIDADE'] = df_resumo['QUANTIDADE'].apply(formatar_quantidade)
    
    st.dataframe(df_resumo, use_container_width=True)

def exibir_grafico_top_produtos(df, periodo_inicial, periodo_final):
    periodo_inicial = pd.to_datetime(periodo_inicial)
    periodo_final = pd.to_datetime(periodo_final)
    df_mes = df.dropna(subset=['Data do Pedido'])
    df_mes = df_mes[(df_mes['Data do Pedido'] >= periodo_inicial) & (df_mes['Data do Pedido'] <= periodo_final)]
    
    if df_mes.empty:
        st.warning("Nenhum dado disponível para o período selecionado (Top Produtos).")
        return
    
    top_produtos = df_mes.groupby('DESCRICAO_1').agg(
        Total_Vendido=('QT', 'sum'),
        Valor_Total_Vendido=('VALOR TOTAL VENDIDO', 'sum')
    ).reset_index()

    top_produtos = top_produtos.sort_values(by='Valor_Total_Vendido', ascending=False).head(20)
    top_produtos['Valor_Total_Vendido_Formatado'] = top_produtos['Valor_Total_Vendido'].apply(formatar_valor)
    top_produtos['Total_Vendido'] = top_produtos['Total_Vendido'].apply(formatar_quantidade)

    fig = px.bar(top_produtos, x='DESCRICAO_1', y='Valor_Total_Vendido',
                 title='Top 20 Produtos Mais Vendidos',
                 labels={'DESCRICAO_1': 'Produto', 'Valor_Total_Vendido': 'Valor Total Vendido (R$)'},
                 color='Valor_Total_Vendido', color_continuous_scale='RdYlGn',
                 hover_data={'DESCRICAO_1': False, 'Valor_Total_Vendido_Formatado': True, 'Total_Vendido': True})

    fig.update_traces(texttemplate="%{customdata}", textposition="outside", textfont_size=12,
                      customdata=top_produtos['Valor_Total_Vendido_Formatado'])
    fig.update_layout(title_font_size=20, xaxis_title_font_size=13, yaxis_title_font_size=13,
                      xaxis_tickfont_size=10, yaxis_tickfont_size=12, xaxis_tickangle=-45)

    st.plotly_chart(fig, use_container_width=True, key=f"top_produtos_{periodo_inicial}_{periodo_final}")

def exibir_grafico_vendas_por_tempo(df, periodo_inicial, periodo_final):
    periodo_inicial = pd.to_datetime(periodo_inicial)
    periodo_final = pd.to_datetime(periodo_final)
    df_periodo = df.dropna(subset=['Data do Pedido'])
    df_periodo = df_periodo[(df_periodo['Data do Pedido'] >= periodo_inicial) & (df_periodo['Data do Pedido'] <= periodo_final)]

    if df_periodo.empty:
        st.warning("Nenhum dado disponível para o período selecionado (Vendas por Tempo).")
        return
    
    vendas_por_mes = df_periodo.groupby(['Ano', 'Mês']).agg(
        Total_Vendido=('QT', 'sum'),
        Valor_Total_Vendido=('VALOR TOTAL VENDIDO', 'sum')
    ).reset_index()

    vendas_por_mes['Mês_Nome'] = vendas_por_mes['Mês'].map({
        1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    })

    vendas_por_mes['Ano'] = vendas_por_mes['Ano'].astype(str)  # Converter Ano para string para evitar problemas no Plotly

    fig = px.line(vendas_por_mes, x='Mês_Nome', y='Valor_Total_Vendido', color='Ano',
                  title='Evolução das Vendas ao Longo do Período',
                  labels={'Mês_Nome': 'Mês', 'Valor_Total_Vendido': 'Valor Total Vendido (R$)', 'Ano': 'Ano'},
                  markers=True)

    fig.update_layout(
        title_font_size=20,
        xaxis_title_font_size=16,
        yaxis_title_font_size=16,
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14,
        xaxis_tickangle=-45
    )

    st.plotly_chart(fig, use_container_width=True, key=f"vendas_por_tempo_{periodo_inicial}_{periodo_final}")

def main():
    
    # Chamar auto_reload para verificar se precisa atualizar
    auto_reload()

    st.title("🍾 Desempenho de Vendas por Produto")
    
    # Carregar dados
    with st.spinner("Carregando dados..."):
        df = carregar_dados()

    if df.empty:
        st.error("Nenhum dado carregado. Verifique o Supabase.")
        return

    # Configuração do período padrão (mês atual: maio de 2025)
    hoje = datetime(2025, 5, 13)
    primeiro_dia_mes = hoje.replace(day=1)
    ultimo_dia_mes = hoje.replace(day=calendar.monthrange(hoje.year, hoje.month)[1])

    # Estilização do campo de pesquisa
    st.markdown("""
    <style>
        .stTextInput>div>div>input {
            border: 2px solid #4CAF50;
            border-radius: 10px;
            padding: 10px;
            font-size: 16px;
            background-color: #f9f9f9;
        }
    </style>
    """, unsafe_allow_html=True)

    # Campo de pesquisa
    produto_pesquisa = st.text_input('🔍 Pesquise por um produto ou código', '', key='search_input')

    # Seção da tabela
    if 'Data do Pedido' in df.columns:
        with st.container():
            st.subheader("Tabela de Resumo")
            col1, col2 = st.columns(2)
            with col1:
                periodo_inicio_tabela = st.date_input('Data de Início - Tabela', value=primeiro_dia_mes, key='inicio_tabela')
            with col2:
                periodo_fim_tabela = st.date_input('Data de Fim - Tabela', value=ultimo_dia_mes, key='fim_tabela')

            if periodo_inicio_tabela > periodo_fim_tabela:
                st.error("A data inicial não pode ser maior que a data final.")
                return
        
            df_filtrado = df.dropna(subset=['Data do Pedido'])
            df_filtrado = df_filtrado[(df_filtrado['Data do Pedido'] >= pd.to_datetime(periodo_inicio_tabela)) & 
                                      (df_filtrado['Data do Pedido'] <= pd.to_datetime(periodo_fim_tabela))]

            if produto_pesquisa:
                produto_pesquisa = ' '.join(produto_pesquisa.split()).strip()
                df_filtrado['DESCRICAO_1'] = df_filtrado['DESCRICAO_1'].apply(lambda x: ' '.join(str(x).split()).strip())
                df_filtrado['CÓDIGO PRODUTO'] = df_filtrado['CÓDIGO PRODUTO'].apply(lambda x: ' '.join(str(x).split()).strip())
                df_filtrado = df_filtrado[
                    df_filtrado['DESCRICAO_1'].str.contains(produto_pesquisa, case=False, na=False) |
                    df_filtrado['CÓDIGO PRODUTO'].str.contains(produto_pesquisa, case=False, na=False)
                ]

            exibir_tabela(df_filtrado)

    # Seção dos gráficos
    with st.container():
        st.subheader("Top Produtos Mais Vendidos por Valor")
        col1, col2 = st.columns(2)
        with col1:
            periodo_inicio_produtos = st.date_input('Data de Início - Top Produtos', value=primeiro_dia_mes, key='inicio_produtos')
        with col2:
            periodo_fim_produtos = st.date_input('Data de Fim - Top Produtos', value=ultimo_dia_mes, key='fim_produtos')

        if periodo_inicio_produtos > periodo_fim_produtos:
            st.error("A data inicial não pode ser maior que a data final.")
            return
        
        exibir_grafico_top_produtos(df, periodo_inicio_produtos, periodo_fim_produtos)

        st.subheader("Evolução das Vendas ao Longo do Período")
        col1, col2 = st.columns(2)
        with col1:
            periodo_inicio_tempo = st.date_input('Data de Início - Vendas por Tempo', value=primeiro_dia_mes, key='inicio_tempo')
        with col2:
            periodo_fim_tempo = st.date_input('Data de Fim - Vendas por Tempo', value=ultimo_dia_mes, key='fim_tempo')

        if periodo_inicio_tempo > periodo_fim_tempo:
            st.error("A data inicial não pode ser maior que a data final.")
            return
        
        exibir_grafico_vendas_por_tempo(df, periodo_inicio_tempo, periodo_fim_tempo)

if __name__ == "__main__":
    main()
