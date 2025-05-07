import streamlit as st
import pandas as pd
from datetime import datetime
import pytz
from io import BytesIO
import concurrent.futures
import re
import math
from snowflake.snowpark import Session

st.set_page_config(
    page_title="App Justificativa",   
    page_icon="📄",                    
    layout="wide"                      
)

@st.cache_resource
def get_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()

session = get_session()

st.markdown("<h1 style='text-align: center;'>JUSTIFICATIVAS BP</h1>", unsafe_allow_html=True)
st.markdown(
    """
    <style>
      /* ===== Scrollbar ===== */
      ::-webkit-scrollbar {
        width: 12px;
        height: 12px;
      }
      ::-webkit-scrollbar-track {
        background: #f0f0f0;
      }
      ::-webkit-scrollbar-thumb {
        background-color: #888;
        border-radius: 6px;
        border: 3px solid #f0f0f0;
      }
      ::-webkit-scrollbar-thumb:hover {
        background-color: #555;
      }

      /* ===== DataFrame container ===== */
      /* Seleciona o grid interno que o st.dataframe renderiza */
      .stDataFrame > div[role="grid"] {
        width: 200px !important;      /* 90% da área disponível */
        margin: 0 auto !important;  /* centraliza horizontalmente */
        height: 800px !important;   /* define uma altura fixa */

      }
    </style>
    """,
    unsafe_allow_html=True
)
@st.cache_data()
def fetch_data(sql):
    results = session.sql(sql).collect()
    if results:
        return session.create_dataframe(results).to_pandas()
    return pd.DataFrame()

# Função para carregar os dados de forma concorrente
@st.cache_data(show_spinner=False)
def load_data_concurrently():
    queries = {
        "df_just_geral": "SELECT * FROM BASES_SPDO.DB_APP_JUST_BP.TB_JUST_GERAL",
        "df_just_status": "SELECT * FROM BASES_SPDO.DB_APP_JUST_BP.TB_JUST_STATUS",
        "df_just_jobs": "SELECT * FROM BASES_SPDO.DB_APP_JUST_BP.TB_JUST_JOBS",
    }

    data_results = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_key = {executor.submit(fetch_data, sql): key for key, sql in queries.items()}
        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            data_results[key] = future.result()
    return data_results

@st.cache_data(show_spinner=False)
def load_just_geral():
    sql = "SELECT * FROM BASES_SPDO.DB_APP_JUST_BP.TB_JUST_GERAL"
    return session.sql(sql).to_pandas()

@st.cache_data(show_spinner=False)
def load_just_status():
    sql = "SELECT * FROM BASES_SPDO.DB_APP_JUST_BP.TB_JUST_STATUS"
    return session.sql(sql).to_pandas()

@st.cache_data(show_spinner=False)
def load_just_jobs():
    sql = "SELECT * FROM BASES_SPDO.DB_APP_JUST_BP.TB_JUST_JOBS"
    return session.sql(sql).to_pandas()

def create_list(df, coluna):
    if df is not None and not df.empty:
        return df[coluna].dropna().unique().tolist()
    return []

def df_to_list(df, coluna, label, placeholder, key=""):
    if df is not None and not df.empty:
        options = create_list(df, coluna)
        return st.multiselect(f"{label}:", options=options, default=[], placeholder=placeholder, key=key)
    else:
        st.warning(f"Nenhum {label} disponível para seleção.")
        return []

def render_justificativas_tab(df_geral, df_status, df_jobs):
    st.markdown("### Visualizar Justificativas com Filtros")
    
    col1, col2, col3 = st.columns(3)

    with col1:                                                                                                                                                                                                                                                                                                                                                  
        ano_atual = datetime.now().year
        ano_list = create_list(df_geral, "ANO")
        default_ano = [ano_atual] if ano_atual in ano_list else []
        
        filter_ano = st.multiselect(
            "Ano:",
            options=ano_list,
            default=default_ano,
            placeholder="Selecione os anos para consulta",
            key="filter_ano"
        )
        
    with col2:
        options_mes = create_list(df_geral, "MES")
        # Dicionário para mapear número para nome do mês (em caixa alta, conforme seu banco)
        meses = {
            1: "JANEIRO",
            2: "FEVEREIRO",
            3: "MARCO",
            4: "ABRIL",
            5: "MAIO",
            6: "JUNHO",
            7: "JULHO",
            8: "AGOSTO",
            9: "SETEMBRO",
            10: "OUTUBRO",
            11: "NOVEMBRO",
            12: "DEZEMBRO"
        }
        mes_atual = meses[datetime.now().month]
        default_mes = [mes_atual] if mes_atual in options_mes else []
        filter_mes = st.multiselect(
            "Mês:",
            options=options_mes,
            default=default_mes,
            placeholder="Selecione os meses para consulta",
            key="filter_mes"
        )

    with col3:
        if not df_geral.empty and "DEC" in df_geral.columns:
            all_dec = sorted(set(";".join(df_geral["DEC"].dropna().astype(str)).split(";")))
        else:
            all_dec = []
        current_day = datetime.now().day
        if current_day <= 10:
            default_dec = "1"
        elif current_day <= 20:
            default_dec = "2"
        else:
            default_dec = "3"
        default_dec = [default_dec] if default_dec in all_dec else []
        filter_dec = st.multiselect(
            "Dec:", 
            options=all_dec, 
            default=default_dec,
            placeholder="Selecione os decêndios para consulta.", 
            key="filter_dec"
        )
    # Cria um DataFrame filtrado para as demais listas
    df_filtered = df_geral.copy()
    if filter_ano:
        df_filtered = df_filtered[df_filtered["ANO"].isin(filter_ano)]
    if filter_mes:
        df_filtered = df_filtered[df_filtered["MES"].isin(filter_mes)]
    if filter_dec:
        df_filtered = df_filtered[df_filtered["DEC"].apply(
            lambda x: any(d in x.split(";") for d in filter_dec) if isinstance(x, str) else False
        )]
        
    col1, col2, col3 = st.columns(3)
    with col1:
        filter_coletor = df_to_list(df_filtered, "COLETOR_BP", "Coletor", "Selecione os Coletores para consulta.", "filter_coletor")
    with col2:
        filter_bp = df_to_list(df_filtered, "BP", "BPs", "Selecione os BPs para consulta.", "filter_bp")
    with col3:
        filter_form = df_to_list(df_filtered, "FORMULARIO_BP", "Formulário", "Selecione os Formulários para consulta", "filter_form")
    col1, col2, col3 = st.columns(3)
    with col1:
        filter_status = df_to_list(df_status, "STATUS", "Status", "Selecione os Status para consulta.", "filter_status")
    with col2:
        filter_just = st.multiselect("Justificativa:", [ "Todos", "Preenchido", "Não Preenchido"], placeholder="Selecione os que possuem justificativa ou não",key="filter_just")
    with col3:
        filter_jobs = df_to_list(df_jobs, "JOBS", "Jobs", "Selecione os Jobs para consulta.", "filter_jobs")
    
    # Conversão para datetime e formatação com hora (DD/MM/YYYY HH:MM:SS)
    df_geral["DATA_JUST"] = pd.to_datetime(df_geral["DATA_JUST"], dayfirst=True)
    
    col_date1, col_date2 = st.columns(2)
    data_inicial_str = col_date1.text_input("Data de Justificativa Inicial (dd/mm/aaaa):", placeholder="DD/MM/AAAA")
    data_final_str = col_date2.text_input("Data de Justificativa Final (dd/mm/aaaa):", placeholder="DD/MM/AAAA")

    if df_geral is not None and not df_geral.empty:
        df_form = df_geral.copy()
        if filter_bp:
            df_form = df_form[df_form["BP"].isin(filter_bp)]
        if filter_jobs:
            df_form = df_form[df_form["JOBS"].apply(
                lambda x: any(d in x.split(";") for d in filter_jobs) if isinstance(x, str) else False
            )]
        if filter_form:
            df_form = df_form[df_form["FORMULARIO_BP"].isin(filter_form)]
        if filter_coletor:
            df_form = df_form[df_form["COLETOR_BP"].isin(filter_coletor)]
        if filter_status:
            df_form = df_form[df_form["STATUS_PESQ"].isin(filter_status)]
        if filter_mes:
            df_form = df_form[df_form["MES"].isin(filter_mes)]
        if filter_ano:
            df_form = df_form[df_form["ANO"].isin(filter_ano)]
        if filter_dec:
            df_form = df_form[df_form["DEC"].apply(
                lambda x: any(d in x.split(";") for d in filter_dec) if isinstance(x, str) else False
            )]
        if filter_just:
            if "Todos" not in filter_just:
                mask_just = pd.Series(False, index=df_form.index)
                if "Preenchido" in filter_just:
                    mask_just |= (
                        df_form["JUSTIFICATIVA"].notna()
                        & (df_form["JUSTIFICATIVA"].str.strip() != "")
                    )
                if "Não Preenchido" in filter_just:
                    mask_just |= (
                        df_form["JUSTIFICATIVA"].isna()
                        | (df_form["JUSTIFICATIVA"].str.strip() == "")
                    )

                df_form = df_form[mask_just]
        date_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}$")
        try:
            if data_inicial_str:
                if not date_pattern.match(data_inicial_str):
                    raise ValueError("Formato inválido")
                start_date = datetime.strptime(data_inicial_str, "%d/%m/%Y")
                df_form = df_form[df_form["DATA_JUST"] >= pd.Timestamp(start_date)]
            if data_final_str:
                if not date_pattern.match(data_final_str):
                    raise ValueError("Formato inválido")
                end_date = datetime.strptime(data_final_str, "%d/%m/%Y")
                end_dt = pd.Timestamp(end_date) + pd.Timedelta(days=1)
                df_form = df_form[df_form["DATA_JUST"] < end_dt]
        except ValueError:
            st.error("Formato de data inválido. Use apenas números e '/' no formato dd/mm/aaaa.")

        # Aqui a formatação inclui data e horário
        if "DATA_JUST" in df_form.columns and not df_form.empty:
            df_form["DATA_JUST"] = df_form["DATA_JUST"].dt.strftime('%d/%m/%Y %H:%M:%S')
        else:
            # Se o DataFrame estiver vazio ou a coluna não existir, cria a coluna com valores vazios
            df_form["DATA_JUST"] = ""
        if "ANO" in df_form.columns and not df_form.empty:
             df_form["ANO"] = df_form["ANO"].astype(str)
        else:
            df_form["ANO"] = ""
        if "BP" in df_form.columns and not df_form.empty:
             df_form["BP"] = df_form["BP"].astype(str)
        else:
            df_form["BP"] = ""

       
        total = df_form.shape[0]
        nao_trabalhados = df_form[df_form["STATUS_PESQ"] == "AINDA NÃO TRABALHADO"].shape[0]
        trabalhados = total - nao_trabalhados

        st.write(f"Total: {total}  |  Trabalhados: {trabalhados}  |  Não trabalhados: {nao_trabalhados}")

        # Reordenando as colunas
        colunas = [
            "ANO", "MES", "DEC", "BP", "DATA_JUST", "COLETOR_BP", "FORMULARIO_BP",
            "JOBS", "COLETOR_PESQ", "FORMULARIO_PESQ", "STATUS_PESQ", "JUSTIFICATIVA"
        ]
        
        
        for col in colunas:
            if col not in df_form.columns:
                df_form[col] = ""
        df_form = df_form[colunas]


        df_sorted = df_form.sort_values(by=["DATA_JUST", "BP"], ascending=[False, True])
        st.dataframe(df_sorted)

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_form.to_excel(writer, index=False, sheet_name="Justificativas")
        output.seek(0)
        st.download_button(
            label="Baixar lista de justificativas",
            data=output,
            file_name="justificativas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.error("Nenhum dado encontrado.")


def render_adicionar_justificativa_tab(df_geral, df_status):
    st.markdown("### Formulário de Justificativa")
    
    # Inicialmente, obtenha as listas de Ano e Mês (essas podem vir do df completo)
    ano_list = create_list(df_geral, "ANO")
    mes_list = create_list(df_geral, "MES")
    
    # Dicionário para mapear o número do mês para o nome em português (caixa alta)
    meses = {
        1: "JANEIRO",
        2: "FEVEREIRO",
        3: "MARÇO",
        4: "ABRIL",
        5: "MAIO",
        6: "JUNHO",
        7: "JULHO",
        8: "AGOSTO",
        9: "SETEMBRO",
        10: "OUTUBRO",
        11: "NOVEMBRO",
        12: "DEZEMBRO"
    }
    
    current_date = datetime.now()
    dia_atual = current_date.day
    mes_atual = meses[current_date.month]
    ano_atual = current_date.year

    if dia_atual <= 10:
        default_dec = ["1"]
    elif dia_atual <= 20:
        default_dec = ["2"]
    else:
        default_dec = ["3"]
    default_mes = [mes_atual] if mes_atual in mes_list else []
    default_ano = [ano_atual] if ano_atual in ano_list else []
    
    col1, col2, col3 = st.columns(3)
    
    # Seleção de Ano, Mês e Dec para filtrar os dados
    selected_anos = col1.multiselect(
        "Ano:",
        options=ano_list,
        default=default_ano,
        help="Selecione um ou mais Anos para adicionar justificativas.",
        key="selected_anos"
    
    )
    selected_mes = col2.multiselect(
        "Mês:",
        options=mes_list,
        default=default_mes,
        help="Selecione um ou mais Meses para adicionar justificativas.",
        key="selected_mes"
    )
    selected_decs = col3.multiselect(
        "Decs:",
        options=["1", "2", "3"],
        default=default_dec,
        help="Filtre por um ou mais decêndios.",
        key="selected_decs"
    )
    
    
    # Filtrar o df_geral com base nos filtros de Ano, Mês e Dec
    df_filtered = df_geral.copy()
    if selected_anos:
        df_filtered = df_filtered[df_filtered["ANO"].isin(selected_anos)]
    if selected_mes:
        df_filtered = df_filtered[df_filtered["MES"].isin(selected_mes)]
    if selected_decs:
        df_filtered = df_filtered[df_filtered["DEC"].apply(
            lambda x: any(d in x.split(";") for d in selected_decs) if isinstance(x, str) else False
        )]
    
    # Gere as listas dinâmicas para Coletores e BP a partir do DataFrame filtrado
    colector_list = create_list(df_filtered, "COLETOR_BP")
    colector_list = [item for item in colector_list if item != 'None']
    bp_list = create_list(df_filtered, "BP")
    
    # Outras listas que não dependem dos filtros continuam vindo do df original
    option_list = create_list(df_geral, "FORMULARIO_BP")
    status_justify_list = create_list(df_status, "STATUS")
    
    col1_under, col2_under, col3_under = st.columns(3)
    selected_colectors = col1_under.multiselect(
        "Coletores:",
        options=colector_list,
        default=[],
        help="Filtre por um ou mais coletores.",
        key="selected_colectors"
        
    )
    selected_bps = col2_under.multiselect(
        "BPs:",
        options=bp_list,
        default=[],
        help="Filtre por um ou mais BPs.",
        key="selected_bps"
    )
    selected_forms = col3_under.multiselect(
        "Formulário:",
        options=option_list,
        default=[],
        help="Filtre por um ou mais formulários",
        key="selected_forms"
    )
    selected_status_pesq = col1_under.multiselect(
        "Status de Pesquisa:",
        options=status_justify_list,
        default=[],
        help="Filtre pelo status de pesquisa.",
        key="selected_status_pesq"
    )

    
    # Agrega todos os filtros (inclusive os de Ano, Mês e Dec) para filtrar o DataFrame de formulários
    selected_filters = [
        ("ANO", selected_anos),
        ("MES", selected_mes),
        ("DEC", selected_decs),
        ("COLETOR_BP", selected_colectors),
        ("BP", selected_bps),
        ("FORMULARIO_BP", selected_forms),
        ("STATUS_PESQ", selected_status_pesq),
    ]
    
    # Se nenhum filtro for selecionado, emite aviso
    if not any(selected for _, selected in selected_filters):
        st.warning("Por favor, selecione ao menos um filtro para visualizar os formulários.")
        return

    df_form = df_geral.copy()
    def _save_and_flag(idx: int, sql_to_run: str):
        session.sql(sql_to_run).collect()
        st.cache_data.clear()
        for widget_key in [
            f"form_pesq-{idx}",
            f"form_status-{idx}",
            f"form_coletor-{idx}",
            f"form_just-{idx}"
        ]:
            st.session_state[widget_key] = "" 
        st.session_state[f"saved_{idx}"] = True

    for coluna, selected in selected_filters:
        if not selected:
            continue
        if coluna == "DEC":
            df_form = df_form[df_form["DEC"].apply(
                lambda x: any(d in x.split(";") for d in selected) if isinstance(x, str) else False
            )]
        else:
            df_form = df_form[df_form[coluna].isin(selected)]
    
    df_form["DATA_JUST"] = pd.to_datetime(df_form["DATA_JUST"], errors="coerce")
    df_form = df_form.sort_values("DATA_JUST", na_position="first")
    
    with st.spinner("Processando dados..."):
        df_latest = df_form.groupby(["BP", "MES"], as_index=False).last()
        df_latest = df_latest[df_latest["STATUS_PESQ"] != "CONCLUÍDA"]
    
    if df_latest.empty:
        st.info("Não há formulários para preencher.")
        return

    total = df_form.shape[0]
    nao_trabalhados = df_form[df_form["STATUS_PESQ"] == "AINDA NÃO TRABALHADO"].shape[0]
    trabalhados = total - nao_trabalhados

    st.write(f"Total: {total}  |  Trabalhados: {trabalhados}  |  Não trabalhados: {nao_trabalhados}")

    page_size = 50
    st.markdown("#### Relação de BPs:")
    total_pages = max(1, math.ceil(len(df_latest) / page_size))
    
    if "current_page_setas" not in st.session_state:
        st.session_state.current_page_setas = 1
    
    if st.session_state.current_page_setas > total_pages:
        st.session_state.current_page_setas = total_pages
        st.rerun()
    
    cols = st.columns([2, 3, 1])
    
    if cols[0].button("◀️", key="prev_page"):
        st.session_state.current_page_setas -= 1

    if cols[2].button("▶️", key="next_page"):
        st.session_state.current_page_setas += 1
        
    st.write(f"Página **{st.session_state.current_page_setas}** de **{total_pages}**")

    
    current_page = st.session_state.current_page_setas
    start_index = (current_page - 1) * page_size
    end_index = start_index + page_size
    df_page = df_latest.iloc[start_index:end_index]

    df_geral["DATA_JUST"] = df_geral["DATA_JUST"].dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    df_form["DATA_JUST"] = df_form["DATA_JUST"].dt.strftime('%d/%m/%Y %H:%M:%S')
    
    # Agrupa os campos de cada formulário em um st.form com clear_on_submit=True
    for index, row in df_page.iterrows():
        ultima_atualizacao = (
            pd.to_datetime(row['DATA_JUST'], utc=True)
            .strftime("%d/%m/%Y %H:%M:%S")
            if pd.notna(row['DATA_JUST']) else "Sem data"
        )
        ultima_just = (
            row['JUSTIFICATIVA'] if pd.notna(row['JUSTIFICATIVA']) else "Sem justificativa"
        )
        
        with st.container(border=True):
            st.markdown(
                f"**BP:** {row['BP']} | **Coletor:** {row['COLETOR_BP']} | "
                f"**Formulário:** {row['FORMULARIO_BP']} | **Mês:** {row['MES']} | "
                f"**Dec:** {row['DEC']} | **Última atualização:** {ultima_atualizacao} | "
                f"**Justificativa atual:** {ultima_just}"
            )
            col1, col2, col3 = st.columns(3)
            form_pesq_val = col1.selectbox(
                "Formulário Pesq.:", options=["", *option_list],
                key=f"form_pesq-{index}"
            )
            form_status_val = col2.selectbox(
                "Status:", options=["", *status_justify_list],
                key=f"form_status-{index}"
            )
            form_coletor_val = col3.selectbox(
                "Coletor Pesq.:", options=["", *colector_list],
                key=f"form_coletor-{index}"
            )
            form_just_val = st.text_area(
                "Justificativa:", max_chars=500, key=f"form_just-{index}"
            )
            just_clean = form_just_val.replace("'", "''")
            fuso  = pytz.timezone("America/Sao_Paulo")
            agora = datetime.now().astimezone(fuso).strftime("%Y-%m-%d %H:%M:%S")
            
            if pd.isna(row["DATA_JUST"]):
                        sql = f"""
                        UPDATE BASES_SPDO.DB_APP_JUST_BP.TB_JUST_GERAL
                           SET DATA_JUST = '{agora}',
                               COLETOR_PESQ = '{form_coletor_val}',
                               FORMULARIO_PESQ = '{form_pesq_val}',
                               STATUS_PESQ = '{form_status_val}',
                               JUSTIFICATIVA = '{just_clean}',
                               ID_JUST = 1
                         WHERE BP = '{row['BP']}'
                           AND MES = '{row['MES']}'
                           AND DATA_JUST IS NULL
                        """
            else:
                        new_id = 1 + (int(row.get("ID_JUST")) if pd.notna(row.get("ID_JUST")) else 0)
                        sql = f"""
                        INSERT INTO BASES_SPDO.DB_APP_JUST_BP.TB_JUST_GERAL
                        (ANO, MES, DATA_JUST, DEC, BP, COLETOR_BP, FORMULARIO_BP, JOBS,
                         COLETOR_PESQ, FORMULARIO_PESQ, STATUS_PESQ, JUSTIFICATIVA, ID_JUST)
                        VALUES
                        ('{row["ANO"]}', '{row["MES"]}', '{agora}', '{row["DEC"]}',
                         '{row["BP"]}', '{row["COLETOR_BP"]}', '{row["FORMULARIO_BP"]}',
                         '{row["JOBS"]}', '{form_coletor_val}', '{form_pesq_val}',
                         '{form_status_val}', '{just_clean}', '{new_id}')
                        """

            
            st.button(
                "Salvar justificativa",
                key=f"submit_{index}",
                on_click=_save_and_flag,
                args=(index, sql)
            )
            if st.session_state.get(f"saved_{index}", False):
                st.success("Justificativa salva com sucesso!")
                st.session_state.pop(f"saved_{index}")

            st.session_state[f"saved_{index}"] = False
            if any([
                st.session_state[f"form_pesq-{index}"],
                st.session_state[f"form_status-{index}"],
                st.session_state[f"form_coletor-{index}"],
                st.session_state[f"form_just-{index}"]
            ]) and not st.session_state.get(f"submit-{index}", False):
                st.warning("Você preencheu campos, mas ainda não salvou a justificativa!")

                    

# ================================
# Execução Principal
# ================================
session.sql("USE WAREHOUSE SPDO").collect()

st.logo('assets/logo_ibre.png')

df_geral  = load_just_geral() 

with st.sidebar:
        if st.button("🔄 Limpar Filtros"):
            for key in [
                "filter_coletor","filter_bp","filter_form",
                "filter_status","filter_just","filter_jobs",
                "filter_mes", "filter_dec",
                "selected_colectors","selected_bps","selected_forms",
                "selected_status_pesq",
                "selected_mes", "selected_decs"
            ]:
                if key in st.session_state:
                    st.session_state[key] = []
            st.rerun()

tabs = st.tabs(["Justificativas", "Adicionar Justificativa"])

with tabs[0]:
     
    df_status = load_just_status() 
    df_jobs   = load_just_jobs()   
    render_justificativas_tab(df_geral, df_status, df_jobs)

with tabs[1]:

    df_status = load_just_status() 
    render_adicionar_justificativa_tab(df_geral, df_status)
