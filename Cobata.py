import streamlit as st
import json
import os
import importlib
from flask import Flask, jsonify, request

st.set_page_config(page_title="COBATA", page_icon="::", layout="wide", initial_sidebar_state="auto",)

app = Flask(__name__)


# Caminho do arquivo JSON para armazenar os dados de login
USER_DATA_FILE = "users.json"

# Lista de páginas disponíveis
PAGES = {
    "Página Inicial": "Página_Inicial",
    "Produto": "Produto",
    "Fornecedor": "Fornecedor",
    "Estoque": "Estoque",
    "Vendedores": "Vendedores",
    "Pedidos": "Pedidos",
    "Pedidos Venda": "Pedidos_Venda",
    "Positivacao": "Positivacao",
}

# Função para carregar dados de usuários do arquivo JSON
def load_users():
    if os.path.exists(USER_DATA_FILE):
        with open(USER_DATA_FILE, "r") as f:
            return json.load(f)
    return {}

# Função para salvar os dados de usuários no arquivo JSON
def save_users(users):
    with open(USER_DATA_FILE, "w") as f:
        json.dump(users, f, indent=4)

# Função para exibir a barra de navegação estilizada na barra lateral
def navigation_bar(selected_page):
    st.markdown("""
                <style>
                /* Barra lateral */
                .sidebar .sidebar-content {
                    background: #ADFF2F;
                    padding: 3rem 0;    
                    width: 50px; /* Largura fixa da barra lateral */
                    
                } 

                .st-emotion-cache-1ibsh2c {
                    width: 100%;
                    padding: 6rem 1rem 10rem;
                    max-width: initial;
                    min-width: auto;
                    background-color: #0e1117;
                }

                .login-container {
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh; /* Ocupa toda a altura da tela */
                flex-direction: column;
            }

                .login-form {
                    padding: 2rem;
                    border-radius: 10px;
                    box-shadow: 0px 4px 10px rgba(0, 0, 0, 0.1);
                    background-color: #ffffff;
                    width: 300px; /* Largura do formulário */
                }

                .login-form h1 {
                    text-align: center;
                    margin-bottom: 1rem;
                    font-size: 1.5rem;
                    color: #333;
                }

                .login-form input {
                    width: 100%;
                    padding: 0.8rem;
                    margin-bottom: 1rem;
                    border-radius: 5px;
                    border: 1px solid #ccc;
                }

                .login-form button {
                    width: 100%;
                    padding: 0.8rem;
                    background-color: #4CAF50;
                    color: white;
                    border: none;
                    border-radius: 5px;
                    cursor: pointer;
                    font-size: 1rem;
                }

                .login-form button:hover {
                    background-color: #45a049;
                }

                .login-form .error {
                    color: red;
                    font-size: 0.9rem;
                    text-align: center;
                }

                .st-emotion-cache-6qob1r {
                    position: relative;
                    height: 100%;               #Configuração de cores da aba laterral
                    width: 100%;
                    overflow: overlay;
                    background-color: #16181c;
                }

                .st-emotion-cache-jh76sn {
                    display: inline-flex;
                    -webkit-box-align: center;
                    align-items: center;
                    -webkit-box-pack: center;
                    justify-content: center;
                    font-weight: 400;
                    padding: 0.25rem 0.75rem;
                    border-radius: 0.5rem;
                    min-height: 2.5rem;         #Configuração do botão da aba lateral
                    margin: 0px;
                    margiin-left: 30px;
                    margin-height: 30px;
                    line-height: 1.6;
                    text-transform: none;
                    font-size: inherit;
                    font-family: inherit;
                    color: inherit;
                    width: 100%;
                    cursor: pointer;
                    user-select: none;
                    background-color: #16181c;
                    border: 1px solid rgba(250, 250, 250, 0.2);

                    }

                    .st-emotion-cache-1espb9k {
                        font-family: "Source Sans Pro", sans-serif;
                        font-size: 1rem;
                        margin-bottom: -1rem;
                        color: inherit;
                        
                    }

                    .st-emotion-cache-1wqrzgl {
                        position: relative;
                        top: 0.125rem;
                        background-color: rgb(38, 39, 48);
                        z-index: 999991;
                        min-width: 244px;
                        max-width: 200px;
                        transform: none;
                        transition: transform 300ms, min-width 300ms, max-width 300ms;
                    }
                
        
                    .st-emotion-cache-xhkv9f  {
                        position: absolute;
                        left: -4%;
	                    top: 0%
                        margin-left:-90px;
		                margin-top:-50px;
                        height: fit-content;
                        width: fit-content;
                        max-width: 100%;
                        display: flex;
                        -webkit-box-pack: center;
                        justify-content: center;
                        pointer-events: none
                    }
                
                    
                    .st-emotion-cache-1espb9k h1 {
                        font-size: 2.5rem;
                        font-weight: 600;
                        padding: 1.25rem 0px 1rem;
                        text-align: center;
                        
                    }

                /* Estilos de Botões */
                .sidebar .sidebar-content .nav-button {
                    display: block;
                    width: 100% !important; /* Força a largura do botão para preencher a barra lateral */
                    margin: 8px 0;
                    padding: 20px; /* Aumenta o padding para botões maiores */
                    font-size: 1.2rem; /* Aumenta o tamanho da fonte */
                    color: #ffffff;
                    background: linear-gradient(135deg, #0072ff, #00c6ff);
                    border: none;
                    border-radius: 12px; /* Borda arredondada */
                    text-align: center; /* Centraliza o texto */
                    white-space: nowrap; /* Evita quebras de linha */
                    box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.15);
                    transition: all 0.3s ease-in-out;
                }

                /* Hover nos botões */
                .sidebar .sidebar-content .nav-button:hover {
                    background: linear-gradient(135deg, #0056b3, #0078c9);
                    box-shadow: 0px 6px 12px rgba(0, 0, 0, 0.25);
                    transform: scale(1.05);
                }

                /* Botão ativo */
                .sidebar .sidebar-content .nav-button.active {
                    background: linear-gradient(135deg, #003d66, #0066cc);
                    box-shadow: 0px 6px 12px rgba(0, 0, 0, 0.35);
                    transform: scale(1.1);
                    border: 2px solid #fff; /* Borda branca no botão ativo */
                }

                /* Responsividade: Para telas menores */
                @media (max-width: 768px) {
                    .sidebar .sidebar-content {
                        width: 150px; /* Ajuste para largura da barra lateral em telas pequenas */
                    }

                    .sidebar .sidebar-content .nav-button {
                        font-size: 1rem; /* Ajusta o tamanho da fonte para telas menores */
                        padding: 15px; /* Ajusta o padding para telas menores */
                        width: 100% !important; /* Ajuste a largura dos botões para preencher a tela */
                    }
                }
                </style>
        """,
        unsafe_allow_html=True,
    )
    
    st.sidebar.image("WhatsApp_Image_2024-11-28_at_10.47.28-removebg-preview.png", width=200 )
    st.sidebar .title("PAINEL")  # Título vazio para não ocupar espaço extra


    if st.sidebar.button("Inicio"):
        st.session_state.page = "Página Inicial"

    st.sidebar.subheader("Logística")
    if "Estoque" in st.session_state.user_permissions:
        if st.sidebar.button("Estoque"):
            st.session_state.page = "Estoque"
    if "Fornecedor" in st.session_state.user_permissions:
        if st.sidebar.button("Fornecedor"):
            st.session_state.page = "Fornecedor"
    if "Pedidos" in st.session_state.user_permissions:
        if st.sidebar.button("Pedidos Separação"):
            st.session_state.page = "Pedidos"

    st.sidebar.subheader("Vendas")
    if "Produto" in st.session_state.user_permissions:
        if st.sidebar.button("Produto"):
            st.session_state.page = "Produto"
    if "Vendedores" in st.session_state.user_permissions:
        if st.sidebar.button("Vendedores"):
            st.session_state.page = "Vendedores"
    if "Pedidos Venda" in st.session_state.user_permissions:
        if st.sidebar.button("Pedidos Venda"):
            st.session_state.page = "Pedidos Venda"
    if "Positivacao" in st.session_state.user_permissions:
        if st.sidebar.button("Positivação"):
            st.session_state.page = "Positivacao"


    
            
# Função para exibir o formulário de login
def login_page():
    st.image("WhatsApp_Image_2024-11-28_at_10.47.28-removebg-preview.png", width=200)

    st.title("Login")
    
    # Campos de login
    username = st.text_input("Nome de usuário")
    password = st.text_input("Senha", type="password")

    # Carregar dados dos usuários
    users_db = load_users()

    # Colunas para os botões lado a lado
    col1, col2 = st.columns([1, 1])

    # Validação do login
    with col1:
        if st.button("Entrar"):
            if username in users_db and users_db[username]["password"] == password:
                # Login bem-sucedido, configurando estado da sessão
                st.session_state.logged_in = True
                st.session_state.user_permissions = users_db[username]["permissions"]
                st.session_state.page = "Página Inicial"  # Define a página inicial




# Função para exibir o formulário de registro de um novo usuário
def register_page():
    st.title("Página de Registro")


    # Carregar dados dos usuários
    users_db = load_users()

# Função para carregar e exibir a página selecionada
def load_page(page_name):
    # Verifica se o usuário tem permissão para acessar a página
    if page_name != "Página Inicial" and page_name not in st.session_state.user_permissions:
        st.error("Você não tem permissão para acessar esta página.")
        return
    
    module_name = PAGES.get(page_name)
    if module_name:
        try:
            page_module = importlib.import_module(module_name)
            page_module.main()  # Presume que cada página tem uma função `main()`
        except ModuleNotFoundError:
            st.error(f"Módulo '{module_name}' não encontrado.")
        except AttributeError:
            st.error(f"O módulo '{module_name}' não possui uma função 'main()'.")

# Função principal que gerencia o fluxo da aplicação
def main():
    # Verifica se a sessão de login existe
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

    if 'page' not in st.session_state:
        st.session_state.page = "Login"  # Inicia com a página de login

    # Controle de navegação
    if st.session_state.logged_in:
        # Renderiza a barra de navegação estilizada
        navigation_bar(st.session_state.page)
        load_page(st.session_state.page)
    else:
        # Exibe a página de login ou registro
        if st.session_state.page == "Login":
            login_page()


if __name__ == "__main__":
    main()