import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import io
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
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
    response = supabase.table('PCVENDEDOR').select('CODUSUR').limit(1).execute()
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

# Função para formatar valores monetários manualmente
def formatar_valor(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# Função principal
def main():
    
    # Chamar auto_reload para verificar se precisa atualizar
    auto_reload()

    # Título
    st.title("Relatório de Vendas e Positivação por Vendedor")

    # Mapeamento de nomes de fornecedores
    default_supplier_names = {
        99678: "JTI", 5832: "PMB", 5065: "SEDAS", 6521: "SEDAS", 99209: "GLOBALBEV",
        999573: "VCT", 91257: "CHIAMULERA", 999574: "MONIN", 99569: "BEAM SUNTORY",
        24: "GALLO", 999571: "BALY", 90671: "KRUG", 99528: "NATIQUE", 60: "PERNOD",
        99502: "BACARDI", 99534: "SALTON", 81: "SALTON", 34: "AURORA", 999579: "AURORA",
        18: "PECCIN", 999577: "FLORESTAL",
    }

    BRITVIC_TEMP_CODE = 999993
    default_supplier_names[BRITVIC_TEMP_CODE] = "BRITVIC"

    # Lista ordenada dos fornecedores
    ordered_suppliers = [
        "GALLO", "GLOBALBEV", "FLORESTAL", "PECCIN", "SEDAS", "JTI", "PMB", "VCT",
        "CHIAMULERA", "MONIN", "BEAM SUNTORY", "AURORA", "SALTON", "BACARDI",
        "PERNOD", "BALY", "KRUG", "NATIQUE", "BRITVIC"
    ]

    # Lista de códigos de produtos associados ao fornecedor BRITVIC
    britvic_product_codes = [
        2798, 1044, 989, 560, 163, 57, 5006, 4988, 4987, 4985, 4415, 4414, 4200, 4199,
        3871, 3870, 3385, 3123, 3058, 2797, 2796, 2795, 2794, 2793, 1047, 58, 5386,
        5385, 5303, 5302, 5301, 5300, 5299, 5298, 5297, 5296, 5295, 5294, 5293, 5292,
        5291, 5290, 5288, 5287, 5286, 5285, 5284, 5283, 5282, 5281, 5280, 5234, 5233,
        5232, 5231, 5230, 5229, 5228, 5227, 5226, 5225, 5224, 5223, 5222, 5221, 5220,
        5219, 5218, 5217, 5216, 5215, 5214, 5213, 5212, 5211, 5210, 5209, 5208, 5207,
        3872, 1038, 988, 278
    ]

    dias_semana_map = {
        "SEGUNDA": 0, "TERCA": 1, "TERÇA": 1, "QUARTA": 2, "QUINTA": 3,
        "SEXTA": 4, "SABADO": 5, "SÁBADO": 5, "DOMINGO": 6
    }

    # Função para verificar se o pedido está dentro da rota
    def is_pedido_dentro_rota(dia_pedido, rota):
        try:
            dia_pedido_num = pd.to_datetime(dia_pedido).weekday()
            rota = rota.upper() if isinstance(rota, str) else ""
            if rota in dias_semana_map:
                return dia_pedido_num == dias_semana_map[rota]
            return False
        except:
            return False

    # Função para buscar dados do Supabase com paginação
    @st.cache_data(show_spinner=False, ttl=60)
    def fetch_data(data_inicial, data_final):
        try:
            all_data = []
            offset = 0
            limit = 1000  # Limite ajustado para evitar sobrecarga

            while True:
                response = supabase.table('PCVENDEDOR').select('*').gte('DATAPEDIDO', data_inicial.strftime("%Y-%m-%d")).lte('DATAPEDIDO', data_final.strftime("%Y-%m-%d")).range(offset, offset + limit - 1).execute()
                response_data = response.data
                if not response_data:
                    logger.info(f"Finalizada a recuperação de dados da tabela PCVENDEDOR")
                    break
                all_data.extend(response_data)
                offset += limit
                logger.info(f"Recuperados {len(response_data)} registros da tabela PCVENDEDOR, total até agora: {len(all_data)}")

            if all_data:
                df = pd.DataFrame(all_data)
                # Verificar colunas obrigatórias
                required_columns = ['DATAPEDIDO', 'VALOR', 'QUANTIDADE', 'CODIGOVENDA', 'CODFORNECEDOR', 
                                   'CODPRODUTO', 'CUSTOPRODUTO', 'PEDIDO', 'CODUSUR', 'VENDEDOR', 
                                   'CODCLIENTE', 'ROTA']
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    logger.error(f"Colunas não encontradas na tabela PCVENDEDOR: {', '.join(missing_columns)}")
                    st.error(f"Colunas não encontradas na tabela PCVENDEDOR: {', '.join(missing_columns)}")
                    return pd.DataFrame()
                return df
            else:
                logger.warning(f"Nenhum dado encontrado na tabela PCVENDEDOR para o período {data_inicial} a {data_final}")
                st.warning(f"Nenhum dado encontrado na tabela PCVENDEDOR para o período {data_inicial} a {data_final}.")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Erro ao buscar dados do Supabase: {e}")
            st.error(f"Erro ao buscar dados do Supabase: {e}")
            return pd.DataFrame()

    # Função para obter dados em cache ou buscar novos
    def get_data(data_inicial, data_final):
        if 'cached_data' not in st.session_state:
            st.session_state.cached_data = None
            st.session_state.cached_range = (None, None)
        
        if (st.session_state.cached_data is not None and
            st.session_state.cached_range[0] <= data_inicial and
            st.session_state.cached_range[1] >= data_final):
            df = st.session_state.cached_data
            return df[(df['DATAPEDIDO'] >= pd.to_datetime(data_inicial)) & 
                     (df['DATAPEDIDO'] <= pd.to_datetime(data_final))]
        else:
            df = fetch_data(data_inicial, data_final)
            if not df.empty:
                df['DATAPEDIDO'] = pd.to_datetime(df['DATAPEDIDO'], errors='coerce')
                df = df.dropna(subset=['DATAPEDIDO'])  # Remover linhas com DATAPEDIDO inválido
                st.session_state.cached_data = df
                st.session_state.cached_range = (data_inicial, data_final)
            return df

    # Processar dados para o relatório resumido
    def process_summary_data(df, data_inicial, data_final):
        if df.empty:
            st.warning("Nenhum dado retornado do Supabase para o período selecionado.")
            return pd.DataFrame(), {}, pd.DataFrame()
        
        required_columns = ['DATAPEDIDO', 'VALOR', 'QUANTIDADE', 'CODIGOVENDA', 'CODFORNECEDOR', 
                           'CODPRODUTO', 'CUSTOPRODUTO', 'PEDIDO', 'CODUSUR', 'VENDEDOR', 
                           'CODCLIENTE', 'ROTA']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.error(f"Colunas obrigatórias ausentes: {', '.join(missing_columns)}")
            return pd.DataFrame(), {}, pd.DataFrame()
        
        # Pré-filtragem para otimizar
        df = df[df['DATAPEDIDO'].between(pd.to_datetime(data_inicial), pd.to_datetime(data_final))].copy()
        
        # Garantir tipos de dados corretos
        df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0).astype('float32')
        df['QUANTIDADE'] = pd.to_numeric(df['QUANTIDADE'], errors='coerce').fillna(0).astype('int32')
        df['CODIGOVENDA'] = pd.to_numeric(df['CODIGOVENDA'], errors='coerce').fillna(1).astype('int32')
        df['CODFORNECEDOR'] = pd.to_numeric(df['CODFORNECEDOR'], errors='coerce').fillna(0).astype('int32')
        df['CODPRODUTO'] = pd.to_numeric(df['CODPRODUTO'], errors='coerce').fillna(0).astype('int32')
        df['CUSTOPRODUTO'] = pd.to_numeric(df['CUSTOPRODUTO'], errors='coerce').fillna(0).astype('float32')
        
        # Remover duplicatas
        df = df.drop_duplicates(subset=['PEDIDO', 'CODPRODUTO'])
        
        # Associar produtos BRITVIC
        df.loc[df['CODPRODUTO'].isin(britvic_product_codes), 'CODFORNECEDOR'] = BRITVIC_TEMP_CODE
        
        # Mapear nomes dos fornecedores
        supplier_map = default_supplier_names
        if 'NOMEFORNECEDOR' in df.columns:
            df['FORNECEDOR'] = df['NOMEFORNECEDOR']
        else:
            df['FORNECEDOR'] = df['CODFORNECEDOR'].map(supplier_map).fillna(df['CODFORNECEDOR'].astype(str))
        
        # Calcular pedidos dentro e fora da rota
        df['DENTRO_ROTA'] = df.apply(lambda row: is_pedido_dentro_rota(row['DATAPEDIDO'], row['ROTA']), axis=1)
        
        # Agrupar pedidos dentro e fora da rota
        pedidos_dentro_rota = df[df['DENTRO_ROTA']].groupby(['CODUSUR', 'VENDEDOR'])['PEDIDO'].nunique().reset_index(name='PEDIDOS_DENTRO_ROTA')
        pedidos_fora_rota = df[~df['DENTRO_ROTA']].groupby(['CODUSUR', 'VENDEDOR'])['PEDIDO'].nunique().reset_index(name='PEDIDOS_FORA_ROTA')
        
        # Obter a data mais antiga
        earliest_date = df.groupby(['CODUSUR', 'VENDEDOR'])['DATAPEDIDO'].min().reset_index()
        earliest_date['DATAPEDIDO'] = earliest_date['DATAPEDIDO'].dt.strftime('%d/%m/%Y')
        
        # Pedidos com bonificação
        pedidos_bonific = df[df['CODIGOVENDA'] != 1].groupby(['CODUSUR', 'VENDEDOR'])['PEDIDO'].nunique().reset_index(name='PEDIDOS_COM_BONIFICACAO')
        
        # Total vendido e custo (excluindo pedidos bonificados)
        bonified_pedidos = df[df['CODIGOVENDA'] != 1]['PEDIDO'].unique()
        df_non_bonific = df[~df['PEDIDO'].isin(bonified_pedidos)]
        
        df_non_bonific['TOTAL_ROW_VENDA'] = df_non_bonific['VALOR'] * df_non_bonific['QUANTIDADE']
        df_non_bonific['TOTAL_ROW_CUSTO'] = df_non_bonific['CUSTOPRODUTO'] * df_non_bonific['QUANTIDADE']
        
        total_vendido = df_non_bonific.groupby(['CODUSUR', 'VENDEDOR'])['TOTAL_ROW_VENDA'].sum().reset_index(name='TOTAL_VENDIDO')
        total_custo = df_non_bonific.groupby(['CODUSUR', 'VENDEDOR'])['TOTAL_ROW_CUSTO'].sum().reset_index(name='TOTAL_CUSTO')
        
        # Positivação
        df_positivacao = df[df['CODFORNECEDOR'].isin(default_supplier_names.keys()) | df['CODPRODUTO'].isin(britvic_product_codes)]
        positivacao = df_positivacao.groupby(['CODUSUR', 'VENDEDOR', 'DATAPEDIDO', 'FORNECEDOR'])['CODCLIENTE'].nunique().reset_index(name='POSITIVACAO')
        positivacao = positivacao.groupby(['CODUSUR', 'VENDEDOR', 'FORNECEDOR'])['POSITIVACAO'].sum().reset_index()
        
        positivacao_pivot = positivacao.pivot_table(
            index=['CODUSUR', 'VENDEDOR'],
            columns='FORNECEDOR',
            values='POSITIVACAO',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        for supplier in ordered_suppliers:
            if supplier not in positivacao_pivot.columns:
                positivacao_pivot[supplier] = 0
        
        positivacao_pivot = positivacao_pivot[['CODUSUR', 'VENDEDOR'] + ordered_suppliers]
        
        # Juntar resultados
        result = pedidos_bonific.merge(total_vendido, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(total_custo, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(positivacao_pivot, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(pedidos_dentro_rota, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(pedidos_fora_rota, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(earliest_date, on=['CODUSUR', 'VENDEDOR'], how='outer')
        
        result['TOTAL'] = result['PEDIDOS_DENTRO_ROTA'].fillna(0) + result['PEDIDOS_FORA_ROTA'].fillna(0)
        result['MARKUP_TOTAL'] = ((result['TOTAL_VENDIDO'] - result['TOTAL_CUSTO']) / result['TOTAL_CUSTO'] * 100).round(2)
        result['MARGEM_TOTAL'] = ((result['TOTAL_VENDIDO'] - result['TOTAL_CUSTO']) / result['TOTAL_VENDIDO'] * 100).round(2)
        
        result['MARKUP_TOTAL'] = result['MARKUP_TOTAL'].replace([float('inf'), -float('inf')], 0).fillna(0)
        result['MARGEM_TOTAL'] = result['MARGEM_TOTAL'].replace([float('inf'), -float('inf')], 0).fillna(0)
        result = result.fillna(0)
        
        # Reordenar colunas
        columns_order = ['DATAPEDIDO', 'CODUSUR', 'VENDEDOR', 'PEDIDOS_DENTRO_ROTA', 'PEDIDOS_FORA_ROTA', 'TOTAL', 
                        'PEDIDOS_COM_BONIFICACAO', 'TOTAL_VENDIDO', 'MARKUP_TOTAL', 'MARGEM_TOTAL'] + ordered_suppliers
        result = result[columns_order]
        
        result['PEDIDOS_COM_BONIFICACAO'] = result['PEDIDOS_COM_BONIFICACAO'].astype(int)
        result['PEDIDOS_DENTRO_ROTA'] = result['PEDIDOS_DENTRO_ROTA'].astype(int)
        result['PEDIDOS_FORA_ROTA'] = result['PEDIDOS_FORA_ROTA'].astype(int)
        result['TOTAL'] = result['TOTAL'].astype(int)
        result['TOTAL_VENDIDO'] = result['TOTAL_VENDIDO'].round(2)
        
        result['TOTAL_VENDIDO'] = result['TOTAL_VENDIDO'].apply(formatar_valor)
        result['MARKUP_TOTAL'] = result['MARKUP_TOTAL'].apply(lambda x: f"{x:.2f}%")
        result['MARGEM_TOTAL'] = result['MARGEM_TOTAL'].apply(lambda x: f"{x:.2f}%")
        
        return result, supplier_map, df

    # Processar dados para a visão detalhada dos pedidos
    def process_detailed_orders(df, data_inicial, data_final, supplier_map):
        if df.empty:
            return pd.DataFrame()
        
        required_columns = ['DATAPEDIDO', 'VALOR', 'QUANTIDADE', 'CUSTOPRODUTO', 'CODPRODUTO', 
                           'CODFORNECEDOR', 'CODIGOVENDA', 'CODCLIENTE', 'CODUSUR', 'VENDEDOR', 'PEDIDO', 'FORNECEDOR']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.error(f"Colunas obrigatórias ausentes: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # Pré-filtragem
        df = df[df['DATAPEDIDO'].between(pd.to_datetime(data_inicial), pd.to_datetime(data_final))].copy()
        
        # Garantir tipos de dados corretos
        df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0).astype('float32')
        df['QUANTIDADE'] = pd.to_numeric(df['QUANTIDADE'], errors='coerce').fillna(0).astype('int32')
        df['CUSTOPRODUTO'] = pd.to_numeric(df['CUSTOPRODUTO'], errors='coerce').fillna(0).astype('float32')
        df['CODPRODUTO'] = pd.to_numeric(df['CODPRODUTO'], errors='coerce').fillna(0).astype('int32')
        df['CODFORNECEDOR'] = pd.to_numeric(df['CODFORNECEDOR'], errors='coerce').fillna(0).astype('int32')
        df['CODIGOVENDA'] = pd.to_numeric(df['CODIGOVENDA'], errors='coerce').fillna(1).astype('int32')
        df['CODCLIENTE'] = pd.to_numeric(df['CODCLIENTE'], errors='coerce').fillna(0).astype('int32')
        
        # Associar produtos BRITVIC
        df.loc[df['CODPRODUTO'].isin(britvic_product_codes), 'CODFORNECEDOR'] = BRITVIC_TEMP_CODE
        df.loc[df['CODPRODUTO'].isin(britvic_product_codes), 'FORNECEDOR'] = 'BRITVIC'
        
        # Definir a coluna FORNECEDOR
        def get_fornecedor(row, supplier_map):
            if row['CODFORNECEDOR'] in supplier_map:
                return supplier_map[row['CODFORNECEDOR']]
            elif 'FORNECEDOR' in df.columns and pd.notnull(row['FORNECEDOR']):
                return row['FORNECEDOR']
            return str(row['CODFORNECEDOR'])
        
        df['FORNECEDOR'] = df.apply(lambda row: get_fornecedor(row, supplier_map), axis=1)
        
        # Determinar bonificação
        df['BONIFICACAO'] = df['CODIGOVENDA'].apply(lambda x: 'Sim' if x != 1 else 'Não')
        
        # Calcular totais
        df['VENDA_TOTAL'] = df['VALOR'] * df['QUANTIDADE']
        df['CUSTO_TOTAL'] = df['CUSTOPRODUTO'] * df['QUANTIDADE']
        df['MARGEM'] = ((df['VENDA_TOTAL'] - df['CUSTO_TOTAL']) / df['VENDA_TOTAL'] * 100).round(2)
        df['MARKUP'] = ((df['VENDA_TOTAL'] - df['CUSTO_TOTAL']) / df['CUSTO_TOTAL'] * 100).round(2)
        df['MARGEM'] = df['MARGEM'].replace([float('inf'), -float('inf')], 0).fillna(0)
        df['MARKUP'] = df['MARKUP'].replace([float('inf'), -float('inf')], 0).fillna(0)
        
        # Formatar DATAPEDIDO
        df['DATAPEDIDO'] = df['DATAPEDIDO'].dt.strftime('%d/%m/%Y')
        
        # Garantir que a coluna PRODUTO existe
        if 'PRODUTO' not in df.columns:
            df['PRODUTO'] = df['CODPRODUTO'].apply(lambda x: f"Produto_{x}")
        
        # Selecionar e renomear colunas
        columns = [
            'DATAPEDIDO', 'CODUSUR', 'VENDEDOR', 'CODCLIENTE', 'PEDIDO',
            'BONIFICACAO', 'QUANTIDADE', 'VALOR', 'CUSTOPRODUTO', 'VENDA_TOTAL', 'CUSTO_TOTAL',
            'MARGEM', 'MARKUP', 'CODPRODUTO', 'PRODUTO', 'FORNECEDOR'
        ]
        available_columns = [col for col in columns if col in df.columns]
        result_df = df[available_columns].copy()
        
        rename_map = {
            'CODCLIENTE': 'CODCLI',
            'VALOR': 'PREÇO',
            'CUSTOPRODUTO': 'CUSTO'
        }
        result_df.rename(columns=rename_map, inplace=True)
        
        # Adicionar colunas indicadoras de fornecedores
        for supplier in ordered_suppliers:
            result_df[supplier] = result_df['FORNECEDOR'].apply(lambda x: 'S' if x == supplier else 'N')
        
        # Formatar colunas numéricas
        result_df['PREÇO'] = result_df['PREÇO'].apply(formatar_valor)
        result_df['CUSTO'] = result_df['CUSTO'].apply(formatar_valor)
        result_df['VENDA_TOTAL'] = result_df['VENDA_TOTAL'].apply(formatar_valor)
        result_df['CUSTO_TOTAL'] = result_df['CUSTO_TOTAL'].apply(formatar_valor)
        result_df['MARGEM'] = result_df['MARGEM'].apply(lambda x: f"{x:.2f}%")
        result_df['MARKUP'] = result_df['MARKUP'].apply(lambda x: f"{x:.2f}%")
        
        # Ordenar
        result_df.sort_values(['DATAPEDIDO', 'VENDEDOR', 'PEDIDO'], inplace=True)
        
        return result_df

    # Processar dados para a tabela de resumo por ano/mês
    def process_year_month_summary(df, selected_year, selected_month):
        if df.empty:
            st.warning("Nenhum dado retornado para o período selecionado (Ano/Mês).")
            return pd.DataFrame()
        
        required_columns = ['DATAPEDIDO', 'VALOR', 'QUANTIDADE', 'CUSTOPRODUTO', 'CODIGOVENDA', 
                           'CODCLIENTE', 'VENDEDOR', 'PEDIDO']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.error(f"Colunas obrigatórias ausentes: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # Pré-filtragem por ano e mês
        df = df[(df['DATAPEDIDO'].dt.year == selected_year) & (df['DATAPEDIDO'].dt.month == selected_month)].copy()
        
        # Garantir tipos de dados corretos
        df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0).astype('float32')
        df['QUANTIDADE'] = pd.to_numeric(df['QUANTIDADE'], errors='coerce').fillna(0).astype('int32')
        df['CUSTOPRODUTO'] = pd.to_numeric(df['CUSTOPRODUTO'], errors='coerce').fillna(0).astype('float32')
        df['CODIGOVENDA'] = pd.to_numeric(df['CODIGOVENDA'], errors='coerce').fillna(1).astype('int32')
        df['CODCLIENTE'] = pd.to_numeric(df['CODCLIENTE'], errors='coerce').fillna(0).astype('int32')
        
        # Excluir pedidos bonificados
        bonified_pedidos = df[df['CODIGOVENDA'] != 1]['PEDIDO'].unique()
        df_non_bonific = df[~df['PEDIDO'].isin(bonified_pedidos)]
        
        # Calcular totais
        df_non_bonific['FATURAMENTO_CLIENTE'] = df_non_bonific['VALOR'] * df_non_bonific['QUANTIDADE']
        df_non_bonific['CUSTO_MERCADORIA'] = df_non_bonific['CUSTOPRODUTO'] * df_non_bonific['QUANTIDADE']
        
        # Agrupar por VENDEDOR, CODCLIENTE
        summary = df_non_bonific.groupby(['CODUSUR', 'VENDEDOR', 'CODCLIENTE']).agg({
            'FATURAMENTO_CLIENTE': 'sum',
            'CUSTO_MERCADORIA': 'sum'
        }).reset_index()
        
        # Calcular margem de contribuição
        summary['CONT_MARG'] = summary['FATURAMENTO_CLIENTE'] - summary['CUSTO_MERCADORIA']
        summary['MARGEM'] = (summary['CONT_MARG'] / summary['FATURAMENTO_CLIENTE'] * 100).round(2)
        
        # Tratar casos extremos
        summary['MARGEM'] = summary['MARGEM'].replace([float('inf'), -float('inf')], 0).fillna(0)
        
        # Formatar colunas numéricas
        summary['CODCLIENTE'] = summary['CODCLIENTE'].astype(int)
        summary['FATURAMENTO_CLIENTE'] = summary['FATURAMENTO_CLIENTE'].apply(formatar_valor)
        summary['CUSTO_MERCADORIA'] = summary['CUSTO_MERCADORIA'].apply(formatar_valor)
        summary['CONT_MARG'] = summary['CONT_MARG'].apply(formatar_valor)
        summary['MARGEM'] = summary['MARGEM'].apply(lambda x: f"{x:.2f}%")
        
        # Ordenar
        summary.sort_values(['VENDEDOR', 'CODCLIENTE'], inplace=True)
        
        return summary

    # Lógica principal do aplicativo
    summary_placeholder = st.empty()
    detailed_placeholder = st.empty()
    year_month_placeholder = st.empty()

    # Intervalo de datas padrão (hoje: 14/05/2025)
    default_start_date = date(2025, 5, 14)
    default_end_date = date(2025, 5, 14)

    # Inicializar estado da sessão para relatórios
    if 'summary_reports' not in st.session_state:
        st.session_state.summary_reports = []
    if 'detailed_reports' not in st.session_state:
        st.session_state.detailed_reports = []
    if 'year_month_summaries' not in st.session_state:
        st.session_state.year_month_summaries = []

    # Limpar relatórios anteriores para evitar duplicação
    st.session_state.summary_reports = []
    st.session_state.detailed_reports = []
    st.session_state.year_month_summaries = []

    # Seção de Tabela Resumida
    with summary_placeholder.container():
    st.subheader("Resultados - Resumo")
    col1, col2 = st.columns(2)
    with col1:
        data_inicial_1 = st.date_input("Data Inicial (Resumo)", value=default_start_date, key="data_inicial_1")
    with col2:
        data_final_1 = st.date_input("Data Final (Resumo)", value=default_end_date, key="data_final_1")
    
    if data_inicial_1 > data_final_1:
        st.error("A data inicial não pode ser maior que a data final.")
        return
    
    st.markdown(f"**Data Inicial:** {data_inicial_1.strftime('%d/%m/%Y')} | **Data Final:** {data_final_1.strftime('%Y%m%d')}")
    
    # Carregar dados automaticamente
    with st.spinner("Carregando resumo..."):
        df = get_data(data_inicial_1, data_final_1)
        if not df.empty:
            result_df, supplier_map, raw_df = process_summary_data(df, data_inicial_1, data_final_1)
            if not result_df.empty:
                st.session_state.summary_reports.append({
                    'data_inicial': data_inicial_1,
                    'data_final': data_final_1,
                    'result_df': result_df,
                    'supplier_map': supplier_map,
                    'raw_df': raw_df
                })
            else:
                st.warning("Nenhum dado processado para o período selecionado (Resumo).")
        else:
            st.warning("Nenhum dado retornado do Supabase (Resumo).")

    # Exibir resumos
    for idx, report in enumerate(st.session_state.summary_reports):
        with st.container():
            result_df = report['result_df']
            
            gb = GridOptionsBuilder.from_dataframe(result_df)
            gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
            gb.configure_column("DATAPEDIDO", header_name="Data Pedido", width=120, filter="agDateColumnFilter")
            gb.configure_column("CODUSUR", header_name="Cód. Usuário", width=100)
            gb.configure_column("VENDEDOR", header_name="Vendedor", width=200, flex=1)
            gb.configure_column("PEDIDOS_DENTRO_ROTA", header_name="Ped. Dentro Rota", width=120)
            gb.configure_column("PEDIDOS_FORA_ROTA", header_name="Ped. Fora Rota", width=120)
            gb.configure_column("TOTAL", header_name="Total Pedidos", width=110)
            gb.configure_column("PEDIDOS_COM_BONIFICACAO", header_name="Ped. c/ Bonif.", width=120)
            gb.configure_column("TOTAL_VENDIDO", header_name="Total Vendido (R$)", width=140)
            gb.configure_column("MARKUP_TOTAL", header_name="Markup Total (%)", width=130)
            gb.configure_column("MARGEM_TOTAL", header_name="Margem Total (%)", width=130)
            for supplier in ordered_suppliers:
                gb.configure_column(supplier, header_name=supplier, width=80, flex=0.5)  # Largura menor para fornecedores
            
            grid_options = gb.build()
            
            # CSS para melhorar a aparência
            st.markdown(
                """
                <style>
                .ag-root-wrapper {
                    width: 100% !important;
                    max-width: 1200px !important;
                    margin: 0 auto;
                }
                .ag-header-cell-label {
                    justify-content: center;
                }
                .ag-cell {
                    padding: 4px 8px !important;
                }
                </style>
                """,
                unsafe_allow_html=True
            )
            
            AgGrid(
                result_df,
                gridOptions=grid_options,
                height=400,
                fit_columns_on_grid_load=True,
                update_mode=GridUpdateMode.NO_UPDATE,
                allow_unsafe_jscode=True
            )
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                result_df.to_excel(writer, index=False, sheet_name='Relatório')
            excel_data = output.getvalue()
            st.download_button(
                label="Baixar Resumo como Excel",
                data=excel_data,
                file_name=f"relatorio_vendas_positivacao_{report['data_inicial'].strftime('%Y%m%d')}_{report['data_final'].strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_summary_{idx}"
            )

    # Seção de Detalhes dos Pedidos
    with detailed_placeholder.container():
        st.subheader("Detalhes dos Pedidos por Vendedor")
        col1, col2 = st.columns(2)
        with col1:
            data_inicial_2 = st.date_input("Data Inicial (Detalhes)", value=default_start_date, key="data_inicial_2")
        with col2:
            data_final_2 = st.date_input("Data Final (Detalhes)", value=default_end_date, key="data_final_2")
        
        if data_inicial_2 > data_final_2:
            st.error("A data inicial não pode ser maior que a data final.")
            return
        
        st.markdown(f"**Data Inicial:** {data_inicial_2.strftime('%d/%m/%Y')} | **Data Final:** {data_final_2.strftime('%Y%m%d')}")
        
        # Carregar dados automaticamente
        with st.spinner("Carregando detalhes..."):
            df = get_data(data_inicial_2, data_final_2)
            if not df.empty:
                _, supplier_map, raw_df = process_summary_data(df, data_inicial_2, data_final_2)
                detailed_df = process_detailed_orders(raw_df, data_inicial_2, data_final_2, supplier_map)
                if not detailed_df.empty:
                    st.session_state.detailed_reports.append({
                        'data_inicial': data_inicial_2,
                        'data_final': data_final_2,
                        'detailed_df': detailed_df,
                        'supplier_map': supplier_map
                    })
                else:
                    st.warning("Nenhum dado detalhado processado para o período selecionado (Detalhes).")
            else:
                st.warning("Nenhum dado retornado do Supabase (Detalhes).")

        # Exibir detalhes
        for idx, report in enumerate(st.session_state.detailed_reports):
            with st.container():
                detailed_df = report['detailed_df']
                
                gb = GridOptionsBuilder.from_dataframe(detailed_df)
                gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
                gb.configure_column("DATAPEDIDO", header_name="Data Pedido", width=120, filter="agDateColumnFilter")
                gb.configure_column("CODUSUR", header_name="Código Usuário", width=140)
                gb.configure_column("VENDEDOR", header_name="Vendedor", width=200)
                gb.configure_column("CODCLI", header_name="Cód. Cliente", width=120)
                gb.configure_column("PEDIDO", header_name="Pedido", width=120)
                gb.configure_column("BONIFICACAO", header_name="Bonificação", width=120)
                gb.configure_column("QUANTIDADE", header_name="Quantidade", width=120)
                gb.configure_column("PREÇO", header_name="Preço (R$)", width=140)
                gb.configure_column("CUSTO", header_name="Custo (R$)", width=140)
                gb.configure_column("VENDA_TOTAL", header_name="Venda Total (R$)", width=160)
                gb.configure_column("CUSTO_TOTAL", header_name="Custo Total (R$)", width=160)
                gb.configure_column("MARGEM", header_name="Margem (%)", width=120)
                gb.configure_column("MARKUP", header_name="Markup (%)", width=120)
                gb.configure_column("CODPRODUTO", header_name="Cód. Produto", width=120)
                gb.configure_column("PRODUTO", header_name="Produto", width=200)
                gb.configure_column("FORNECEDOR", header_name="Fornecedor", width=150)
                for supplier in ordered_suppliers:
                    gb.configure_column(supplier, header_name=supplier, width=80)
                
                grid_options = gb.build()
                
                AgGrid(
                    detailed_df,
                    gridOptions=grid_options,
                    height=500,
                    fit_columns_on_grid_load=True,
                    update_mode=GridUpdateMode.NO_UPDATE,
                    allow_unsafe_jscode=True
                )
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    detailed_df.to_excel(writer, index=False, sheet_name='Detalhes_Pedidos')
                excel_data = output.getvalue()
                st.download_button(
                    label="Baixar Detalhes como Excel",
                    data=excel_data,
                    file_name=f"detalhes_pedidos_{report['data_inicial'].strftime('%Y%m%d')}_{report['data_final'].strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_detailed_{idx}"
                )

    with detailed_placeholder.container():
    st.subheader("Detalhes dos Pedidos por Vendedor")
    col1, col2 = st.columns(2)
    with col1:
        data_inicial_2 = st.date_input("Data Inicial (Detalhes)", value=default_start_date, key="data_inicial_2")
    with col2:
        data_final_2 = st.date_input("Data Final (Detalhes)", value=default_end_date, key="data_final_2")
    
    if data_inicial_2 > data_final_2:
        st.error("A data inicial não pode ser maior que a data final.")
        return
    
    st.markdown(f"**Data Inicial:** {data_inicial_2.strftime('%d/%m/%Y')} | **Data Final:** {data_final_2.strftime('%Y%m%d')}")
    
    # Carregar dados automaticamente
    with st.spinner("Carregando detalhes..."):
        df = get_data(data_inicial_2, data_final_2)
        if not df.empty:
            _, supplier_map, raw_df = process_summary_data(df, data_inicial_2, data_final_2)
            detailed_df = process_detailed_orders(raw_df, data_inicial_2, data_final_2, supplier_map)
            if not detailed_df.empty:
                st.session_state.detailed_reports.append({
                    'data_inicial': data_inicial_2,
                    'data_final': data_final_2,
                    'detailed_df': detailed_df,
                    'supplier_map': supplier_map
                })
            else:
                st.warning("Nenhum dado detalhado processado para o período selecionado (Detalhes).")
        else:
            st.warning("Nenhum dado retornado do Supabase (Detalhes).")

    # Exibir detalhes
    for idx, report in enumerate(st.session_state.detailed_reports):
        with st.container():
            detailed_df = report['detailed_df']
            
            gb = GridOptionsBuilder.from_dataframe(detailed_df)
            gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
            gb.configure_column("DATAPEDIDO", header_name="Data Pedido", width=120, filter="agDateColumnFilter")
            gb.configure_column("CODUSUR", header_name="Cód. Usuário", width=100)
            gb.configure_column("VENDEDOR", header_name="Vendedor", width=200, flex=1)
            gb.configure_column("CODCLI", header_name="Cód. Cliente", width=100)
            gb.configure_column("PEDIDO", header_name="Pedido", width=100)
            gb.configure_column("BONIFICACAO", header_name="Bonificação", width=100)
            gb.configure_column("QUANTIDADE", header_name="Quantidade", width=100)
            gb.configure_column("PREÇO", header_name="Preço (R$)", width=120)
            gb.configure_column("CUSTO", header_name="Custo (R$)", width=120)
            gb.configure_column("VENDA_TOTAL", header_name="Venda Total (R$)", width=140)
            gb.configure_column("CUSTO_TOTAL", header_name="Custo Total (R$)", width=140)
            gb.configure_column("MARGEM", header_name="Margem (%)", width=110)
            gb.configure_column("MARKUP", header_name="Markup (%)", width=110)
            gb.configure_column("CODPRODUTO", header_name="Cód. Produto", width=100)
            gb.configure_column("PRODUTO", header_name="Produto", width=200, flex=1)
            gb.configure_column("FORNECEDOR", header_name="Fornecedor", width=150, flex=1)
            for supplier in ordered_suppliers:
                gb.configure_column(supplier, header_name=supplier, width=70, flex=0.5)
            
            grid_options = gb.build()
            
            # CSS para melhorar a aparência
            st.markdown(
                """
                <style>
                .ag-root-wrapper {
                    width: 100% !important;
                    max-width: 1400px !important;
                    margin: 0 auto;
                }
                .ag-header-cell-label {
                    justify-content: center;
                }
                .ag-cell {
                    padding: 4px 8px !important;
                }
                </style>
                """,
                unsafe_allow_html=True
            )
            
            AgGrid(
                detailed_df,
                gridOptions=grid_options,
                height=500,
                fit_columns_on_grid_load=True,
                update_mode=GridUpdateMode.NO_UPDATE,
                allow_unsafe_jscode=True
            )
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                detailed_df.to_excel(writer, index=False, sheet_name='Detalhes_Pedidos')
            excel_data = output.getvalue()
            st.download_button(
                label="Baixar Detalhes como Excel",
                data=excel_data,
                file_name=f"detalhes_pedidos_{report['data_inicial'].strftime('%Y%m%d')}_{report['data_final'].strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_detailed_{idx}"
            )

if __name__ == "__main__":
    main()
