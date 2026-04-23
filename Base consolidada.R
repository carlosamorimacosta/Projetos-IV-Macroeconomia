# =============================================================================
# SCRIPT: Consolidação de Dados Agropecuários e de Siderurgia
# Agrupa arquivos Excel/CSV por tema em um único workbook
#
# PLANILHAS GERADAS:
#   1. Precos_Commodities  — CEPEA (boi, etanol, milho, soja, suíno, açúcar,
#                             café arábica, café robusta) + laranja + manga
#   2. Exportacoes         — EXP_ISIC, EXP_Café, EXP_Milho, EXP_Soja +
#                             abas de exportação do TOTAIS_MENSAL
#   3. Importacoes         — IMP_ISIC + abas de importação do TOTAIS_MENSAL
#   4. PIB_Emprego_Soja    — Todas as abas do arquivo de cadeia da soja/biodiesel
#   5. Siderurgia          — dados_gerais.csv (anual) + dados_atuais.csv (mensal)
# =============================================================================


# --- 1. PACOTES --------------------------------------------------------------
pacotes <- c("readxl", "openxlsx", "dplyr", "tidyr", "stringr",
             "purrr", "lubridate", "readr")
instalar <- pacotes[!pacotes %in% installed.packages()[, "Package"]]
if (length(instalar) > 0) install.packages(instalar, dependencies = TRUE)
invisible(lapply(pacotes, library, character.only = TRUE))


# --- 2. CAMINHOS DOS ARQUIVOS ------------------------------------------------
# AJUSTE os caminhos abaixo conforme a localização dos seus arquivos.

PASTA      <- "C:/Users/carlo/Downloads/3º ano - EESP/Projetos IV/Base de dados - aula 3"   # <-- ALTERE AQUI

CSV_GERAIS <- file.path(PASTA, "dados_gerais.csv")   # serie anual  — SH2=72
CSV_ATUAIS <- file.path(PASTA, "dados_atuais.csv")   # serie mensal — SH2=72

SAIDA      <- file.path(PASTA, "Dados_Consolidados.xlsx")

arquivos <- list(
  # Preços CEPEA — aceita par .xls + .xlsx; usa o primeiro que existir
  boi_gordo      = c(file.path(PASTA, "CEPEA_BoiGordo.xls"),
                     file.path(PASTA, "CEPEA_BoiGordo.xlsx")),
  etanol         = c(file.path(PASTA, "CEPEA_Etanol.xls"),
                     file.path(PASTA, "CEPEA_Etanol.xlsx")),
  milho          = c(file.path(PASTA, "CEPEA_Milho.xls"),
                     file.path(PASTA, "CEPEA_Milho.xlsx")),
  soja_paranagua = c(file.path(PASTA, "CEPEA_SojaParanagua.xls"),
                     file.path(PASTA, "CEPEA_SojaParanagua.xlsx")),
  suino          = c(file.path(PASTA, "CEPEA_Suino.xls"),
                     file.path(PASTA, "CEPEA_Suino.xlsx")),
  acucar         = c(file.path(PASTA, "CEPEA_açucar.xls"),
                     file.path(PASTA, "CEPEA_açucar.xlsx")),
  cafe_arabica   = c(file.path(PASTA, "CEPEA_cafearabica.xls"),
                     file.path(PASTA, "CEPEA_cafearabica.xlsx")),
  cafe_robusta   =   file.path(PASTA, "CEPEA_caferobusta.xls"),
  laranja        =   file.path(PASTA, "laranja-precos-medios.xlsx"),
  manga          =   file.path(PASTA, "manga-precos-medios.xlsx"),
  
  # Exportações
  exp_isic       = file.path(PASTA, "EXP_ISIC_MENSAL.xlsx"),
  exp_cafe       = file.path(PASTA, "EXP_MENSAL_Café.xlsx"),
  exp_milho      = file.path(PASTA, "EXP_MENSAL_Milho.xlsx"),
  exp_soja       = file.path(PASTA, "EXP_MENSAL_Soja.xlsx"),
  totais         = file.path(PASTA, "TOTAIS_MENSAL.xlsx"),
  
  # Importações
  imp_isic       = file.path(PASTA, "IMP_ISIC_MENSAL.xlsx"),
  
  # PIB, Emprego e Comércio Exterior — Cadeia da Soja / Biodiesel
  pib_soja       = file.path(PASTA,
                             "Dados PIB, emprego e comércio exterior - cadeia da soja e do biodiesel - ENVIO (5).xlsx")
)


# --- 3. FUNÇÕES AUXILIARES ---------------------------------------------------

# Resolve caminho: aceita vetor de candidatos, retorna o primeiro existente
resolver_arquivo <- function(caminhos) {
  caminhos <- unlist(caminhos)
  existentes <- caminhos[file.exists(caminhos)]
  if (length(existentes) == 0) return(NULL)
  existentes[1]
}

# Lê a primeira aba de um Excel de forma segura
ler_excel_seguro <- function(caminho, sheet = 1, skip = 0) {
  tryCatch(
    read_excel(caminho, sheet = sheet, skip = skip, col_types = "text"),
    error = function(e) {
      message("  [AVISO] Erro ao ler '", basename(caminho), "': ", conditionMessage(e))
      NULL
    }
  )
}

# Lê TODOS os sheets de um Excel e os empilha com coluna aba_origem
ler_todas_abas <- function(caminho, fonte = "") {
  if (is.null(caminho) || !file.exists(caminho)) return(NULL)
  abas <- tryCatch(excel_sheets(caminho), error = function(e) NULL)
  if (is.null(abas)) return(NULL)
  lista <- map(abas, function(aba) {
    df <- ler_excel_seguro(caminho, sheet = aba)
    if (!is.null(df)) df <- mutate(df, aba_origem = aba, .before = 1)
    df
  })
  bind_rows(Filter(Negate(is.null), lista))
}

# Limpeza genérica de um data.frame Excel
limpar_df <- function(df, fonte = "") {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  
  df <- df[, colSums(!is.na(df)) > 0, drop = FALSE]
  df <- df[rowSums(!is.na(df)) > 0, , drop = FALSE]
  
  names(df) <- names(df) %>%
    str_trim() %>%
    str_squish() %>%
    str_replace_all("[^[:alnum:]_]", "_") %>%
    str_replace_all("_{2,}", "_") %>%
    str_replace_all("^_|_$", "") %>%
    str_to_lower()
  
  header_row <- apply(df, 1, function(r) all(r == names(df), na.rm = TRUE))
  df <- df[!header_row, , drop = FALSE]
  
  if (nchar(fonte) > 0) df <- mutate(df, fonte_arquivo = fonte, .before = 1)
  df
}

# Detecta e converte colunas de data
converter_datas <- function(df) {
  if (is.null(df)) return(NULL)
  for (col in names(df)) {
    if (str_detect(tolower(col),
                   "^(data|date|mes|m_s|ano|year|periodo|per_odo)$")) {
      df[[col]] <- suppressWarnings(
        parse_date_time(as.character(df[[col]]),
                        orders = c("dmy", "mdy", "ymd", "my", "ym", "Y"),
                        quiet  = TRUE)
      )
    }
  }
  df
}


# --- 4. LEITURA E LIMPEZA POR GRUPO ------------------------------------------

# ---- 4.1 Preços de Commodities ----------------------------------------------
message("\n=== GRUPO 1: Preços de Commodities ===")

cepea_cfg <- list(
  list(key = "boi_gordo",      fonte = "CEPEA_BoiGordo"),
  list(key = "etanol",         fonte = "CEPEA_Etanol"),
  list(key = "milho",          fonte = "CEPEA_Milho"),
  list(key = "soja_paranagua", fonte = "CEPEA_SojaParanagua"),
  list(key = "suino",          fonte = "CEPEA_Suino"),
  list(key = "acucar",         fonte = "CEPEA_Acucar"),
  list(key = "cafe_arabica",   fonte = "CEPEA_CafeArabica"),
  list(key = "cafe_robusta",   fonte = "CEPEA_CafeRobusta"),
  list(key = "laranja",        fonte = "Laranja_PrecosMedios"),
  list(key = "manga",          fonte = "Manga_PrecosMedios")
)

lst_precos <- map(cepea_cfg, function(cfg) {
  cam <- resolver_arquivo(arquivos[[cfg$key]])
  if (is.null(cam)) { message("  [AVISO] Não encontrado: ", cfg$fonte); return(NULL) }
  message("  Lendo: ", basename(cam))
  ler_excel_seguro(cam) %>% limpar_df(cfg$fonte)
})

df_precos <- bind_rows(Filter(Negate(is.null), lst_precos)) %>% converter_datas()
message("  → Linhas em Precos_Commodities: ", nrow(df_precos))


# ---- 4.2 Exportações --------------------------------------------------------
message("\n=== GRUPO 2: Exportações ===")

df_totais_raw <- ler_todas_abas(resolver_arquivo(arquivos$totais), "TOTAIS_MENSAL")

ler_exp <- function(key, fonte) {
  cam <- resolver_arquivo(arquivos[[key]])
  if (is.null(cam)) return(NULL)
  message("  Lendo: ", basename(cam))
  ler_excel_seguro(cam) %>% limpar_df(fonte)
}

lst_exp <- list(
  ler_exp("exp_isic",  "EXP_ISIC_MENSAL"),
  ler_exp("exp_cafe",  "EXP_MENSAL_Cafe"),
  ler_exp("exp_milho", "EXP_MENSAL_Milho"),
  ler_exp("exp_soja",  "EXP_MENSAL_Soja")
)

if (!is.null(df_totais_raw)) {
  abas_exp <- filter(df_totais_raw, str_detect(tolower(aba_origem), "exp"))
  if (nrow(abas_exp) > 0) lst_exp <- c(lst_exp, list(limpar_df(abas_exp, "TOTAIS_Exp")))
}

df_exportacoes <- bind_rows(Filter(Negate(is.null), lst_exp)) %>% converter_datas()
message("  → Linhas em Exportacoes: ", nrow(df_exportacoes))


# ---- 4.3 Importações --------------------------------------------------------
message("\n=== GRUPO 3: Importações ===")

lst_imp <- list({
  cam <- resolver_arquivo(arquivos$imp_isic)
  if (!is.null(cam)) { message("  Lendo: ", basename(cam))
    ler_excel_seguro(cam) %>% limpar_df("IMP_ISIC_MENSAL") }
})

if (!is.null(df_totais_raw)) {
  abas_imp <- filter(df_totais_raw, str_detect(tolower(aba_origem), "imp"))
  if (nrow(abas_imp) > 0) lst_imp <- c(lst_imp, list(limpar_df(abas_imp, "TOTAIS_Imp")))
}

df_importacoes <- bind_rows(Filter(Negate(is.null), lst_imp)) %>% converter_datas()
message("  → Linhas em Importacoes: ", nrow(df_importacoes))


# ---- 4.4 PIB, Emprego e Cadeia da Soja --------------------------------------
message("\n=== GRUPO 4: PIB, Emprego e Cadeia da Soja ===")

cam_pib <- resolver_arquivo(arquivos$pib_soja)
if (!is.null(cam_pib)) message("  Lendo: ", basename(cam_pib))

df_pib_soja <- if (!is.null(cam_pib)) {
  ler_todas_abas(cam_pib, "PIB_Soja") %>% limpar_df() %>% converter_datas()
} else NULL

message("  → Linhas em PIB_Emprego_Soja: ",
        if (is.null(df_pib_soja)) 0L else nrow(df_pib_soja))


# ---- 4.5 Siderurgia — CSVs --------------------------------------------------
# dados_gerais.csv : exportações anuais  — SH2 72 (Ferro fundido, ferro e aço)
# dados_atuais.csv : exportações mensais — SH2 72 (mesmo tema, inclui coluna Mês)
# Ambos são unidos numa única planilha "Siderurgia".
# Diferenças de colunas entre os dois são preenchidas com NA automaticamente.

message("\n=== GRUPO 5: Siderurgia (CSV) ===")

limpar_csv_siderurgia <- function(caminho, tipo_serie) {
  if (!file.exists(caminho)) {
    message("  [AVISO] Não encontrado: ", caminho)
    return(NULL)
  }
  message("  Lendo: ", basename(caminho), " (", tipo_serie, ")")
  
  # Tenta UTF-8; se falhar, tenta latin1
  df <- tryCatch(
    read_csv(caminho, locale = locale(encoding = "UTF-8"),
             col_types = cols(.default = col_character()),
             show_col_types = FALSE),
    error = function(e) tryCatch(
      read_csv(caminho, locale = locale(encoding = "latin1"),
               col_types = cols(.default = col_character()),
               show_col_types = FALSE),
      error = function(e2) {
        message("  [ERRO] Não foi possível ler: ", conditionMessage(e2))
        NULL
      }
    )
  )
  
  if (is.null(df) || nrow(df) == 0) return(NULL)
  
  # Remove BOM (\uFEFF) do nome da 1ª coluna, gerado pelo Excel ao exportar CSV
  names(df)[1] <- str_replace(names(df)[1], "\uFEFF", "")
  
  # Remove \r residual e espaços desnecessários nos campos de texto
  df <- mutate(df, across(where(is.character),
                          ~ str_replace_all(.x, "\r", "") %>% str_trim()))
  
  # Padroniza nomes de colunas
  names(df) <- names(df) %>%
    str_trim() %>%
    str_replace_all("[^[:alnum:]_]", "_") %>%
    str_replace_all("_{2,}", "_") %>%
    str_replace_all("^_|_$", "") %>%
    str_to_lower()
  
  # Remove linhas completamente vazias
  df <- df[rowSums(!is.na(df) & df != "") > 0, , drop = FALSE]
  
  # Colunas de controle
  mutate(df,
         fonte_arquivo = basename(caminho),
         tipo_serie    = tipo_serie,
         .before       = 1)
}

df_sid_gerais <- limpar_csv_siderurgia(CSV_GERAIS, "anual")
df_sid_atuais <- limpar_csv_siderurgia(CSV_ATUAIS, "mensal")

# bind_rows preenche com NA onde colunas diferem entre os dois arquivos
df_siderurgia <- bind_rows(df_sid_gerais, df_sid_atuais)
message("  → Linhas em Siderurgia: ", nrow(df_siderurgia))


# --- 5. EXPORTAÇÃO PARA EXCEL ------------------------------------------------
message("\n=== Escrevendo: ", SAIDA, " ===")

wb <- createWorkbook()

estilo_header <- createStyle(
  fontName       = "Arial",
  fontSize       = 11,
  fontColour     = "white",
  fgFill         = "#2E5FA3",
  halign         = "center",
  textDecoration = "bold",
  border         = "Bottom"
)
estilo_corpo <- createStyle(fontName = "Arial", fontSize = 10)
estilo_data  <- createStyle(fontName = "Arial", fontSize = 10, numFmt = "DD/MM/YYYY")

adicionar_planilha <- function(wb, nome, df) {
  if (is.null(df) || nrow(df) == 0) {
    message("  [SKIP] Planilha '", nome, "' sem dados — não criada.")
    return(invisible(NULL))
  }
  addWorksheet(wb, nome)
  writeData(wb, nome, df, startRow = 1, startCol = 1, headerStyle = estilo_header)
  addStyle(wb, nome, estilo_corpo,
           rows = 2:(nrow(df) + 1), cols = 1:ncol(df), gridExpand = TRUE)
  for (j in seq_along(df)) {
    if (inherits(df[[j]], c("POSIXct", "POSIXlt", "Date"))) {
      addStyle(wb, nome, estilo_data,
               rows = 2:(nrow(df) + 1), cols = j, gridExpand = TRUE)
    }
  }
  setColWidths(wb, nome, cols = 1:ncol(df), widths = "auto")
  freezePane(wb, nome, firstRow = TRUE)
  message("  Planilha '", nome, "' → ",
          nrow(df), " linhas × ", ncol(df), " colunas")
}

adicionar_planilha(wb, "Precos_Commodities", df_precos)
adicionar_planilha(wb, "Exportacoes",        df_exportacoes)
adicionar_planilha(wb, "Importacoes",        df_importacoes)
adicionar_planilha(wb, "PIB_Emprego_Soja",   df_pib_soja)
adicionar_planilha(wb, "Siderurgia",         df_siderurgia)

saveWorkbook(wb, SAIDA, overwrite = TRUE)
message("\n✅ Arquivo salvo em: ", SAIDA)


# --- 6. SUMÁRIO FINAL --------------------------------------------------------
cat("\n============= SUMÁRIO =============\n")
resumo <- list(
  Precos_Commodities = nrow(df_precos),
  Exportacoes        = nrow(df_exportacoes),
  Importacoes        = nrow(df_importacoes),
  PIB_Emprego_Soja   = if (is.null(df_pib_soja)) 0L else nrow(df_pib_soja),
  Siderurgia         = nrow(df_siderurgia)
)
for (nm in names(resumo))
  cat(sprintf("  %-28s %s linhas\n", paste0(nm, ":"), resumo[[nm]]))
cat("====================================\n")
cat("Saída:", SAIDA, "\n")
