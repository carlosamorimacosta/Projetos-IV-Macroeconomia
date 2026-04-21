# =============================================================================
# SCRIPT: Consolidação de Dados Agropecuários
# Agrupa arquivos Excel por tema em um único workbook
# =============================================================================

# --- 1. PACOTES --------------------------------------------------------------
pacotes <- c("readxl", "openxlsx", "dplyr", "tidyr", "stringr", "purrr", "lubridate")
instalar <- pacotes[!pacotes %in% installed.packages()[, "Package"]]
if (length(instalar) > 0) install.packages(instalar, dependencies = TRUE)
invisible(lapply(pacotes, library, character.only = TRUE))


# --- 2. CAMINHOS DOS ARQUIVOS ------------------------------------------------
# AJUSTE os caminhos abaixo conforme a localização dos seus arquivos
PASTA <- "C:/Users/carlo/Downloads/3º ano - EESP/Projetos IV/Base de dados - aula 3"   # <-- ALTERE AQUI

arquivos <- list(
  # Preços CEPEA (pares .xls + .xlsx — o script usa o que existir)
  boi_gordo       = c(file.path(PASTA, "CEPEA_BoiGordo.xls"),
                      file.path(PASTA, "CEPEA_BoiGordo.xlsx")),
  etanol          = c(file.path(PASTA, "CEPEA_Etanol.xls"),
                      file.path(PASTA, "CEPEA_Etanol.xlsx")),
  milho           = c(file.path(PASTA, "CEPEA_Milho.xls"),
                      file.path(PASTA, "CEPEA_Milho.xlsx")),
  soja_paranagua  = c(file.path(PASTA, "CEPEA_SojaParanagua.xls"),
                      file.path(PASTA, "CEPEA_SojaParanagua.xlsx")),
  suino           = c(file.path(PASTA, "CEPEA_Suino.xls"),
                      file.path(PASTA, "CEPEA_Suino.xlsx")),
  acucar          = c(file.path(PASTA, "CEPEA_açucar.xls"),
                      file.path(PASTA, "CEPEA_açucar.xlsx")),
  cafe_arabica    = c(file.path(PASTA, "CEPEA_cafearabica.xls"),
                      file.path(PASTA, "CEPEA_cafearabica.xlsx")),
  cafe_robusta    = file.path(PASTA, "CEPEA_caferobusta.xls"),
  laranja         = file.path(PASTA, "laranja-precos-medios.xlsx"),
  manga           = file.path(PASTA, "manga-precos-medios.xlsx"),
  
  # Exportações
  exp_isic        = file.path(PASTA, "EXP_ISIC_MENSAL.xlsx"),
  exp_cafe        = file.path(PASTA, "EXP_MENSAL_Café.xlsx"),
  exp_milho       = file.path(PASTA, "EXP_MENSAL_Milho.xlsx"),
  exp_soja        = file.path(PASTA, "EXP_MENSAL_Soja.xlsx"),
  totais          = file.path(PASTA, "TOTAIS_MENSAL.xlsx"),
  
  # Importações
  imp_isic        = file.path(PASTA, "IMP_ISIC_MENSAL.xlsx"),
  
  # PIB, Emprego e Comércio Exterior – Cadeia da Soja
  pib_soja        = file.path(PASTA,
                              "Dados PIB, emprego e comércio exterior - cadeia da soja e do biodiesel - ENVIO (5).xlsx")
)


# --- 3. FUNÇÕES AUXILIARES ---------------------------------------------------

# Resolve o caminho: aceita vetor de candidatos e retorna o primeiro existente
resolver_arquivo <- function(caminhos) {
  caminhos <- unlist(caminhos)
  existentes <- caminhos[file.exists(caminhos)]
  if (length(existentes) == 0) return(NULL)
  existentes[1]
}

# Lê a primeira aba de um arquivo Excel de forma segura
ler_excel_seguro <- function(caminho, sheet = 1, skip = 0) {
  tryCatch({
    df <- read_excel(caminho, sheet = sheet, skip = skip, col_types = "text")
    df
  }, error = function(e) {
    message("  [AVISO] Erro ao ler '", basename(caminho), "': ", conditionMessage(e))
    NULL
  })
}

# Limpeza genérica de um data.frame
limpar_df <- function(df, fonte = "") {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  
  # Remove colunas e linhas totalmente vazias
  df <- df[, colSums(!is.na(df)) > 0]
  df <- df[rowSums(!is.na(df)) > 0, ]
  
  # Padroniza nomes de colunas
  names(df) <- names(df) %>%
    str_trim() %>%
    str_squish() %>%
    str_replace_all("[^[:alnum:]_]", "_") %>%
    str_replace_all("_{2,}", "_") %>%
    str_to_lower()
  
  # Remove linhas que são só cabeçalho repetido (todas células == nome da col)
  header_row <- apply(df, 1, function(r) all(r == names(df), na.rm = TRUE))
  df <- df[!header_row, ]
  
  # Adiciona coluna de origem
  if (fonte != "") df <- mutate(df, fonte_arquivo = fonte, .before = 1)
  
  df
}

# Lê TODOS os sheets de um arquivo e os empilha
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

# Tenta detectar e converter colunas de data
converter_datas <- function(df) {
  for (col in names(df)) {
    if (str_detect(tolower(col), "data|date|mes|mês|ano|year|periodo|período")) {
      df[[col]] <- suppressWarnings(
        parse_date_time(df[[col]],
                        orders = c("dmy", "mdy", "ymd", "my", "ym", "Y"),
                        quiet = TRUE)
      )
    }
  }
  df
}

# Pipeline completo para um arquivo simples (1 aba principal)
processar_arquivo <- function(caminhos, fonte, skip = 0) {
  caminho <- resolver_arquivo(caminhos)
  if (is.null(caminho)) {
    message("  [AVISO] Arquivo não encontrado: ", paste(unlist(caminhos), collapse = " | "))
    return(NULL)
  }
  message("  Lendo: ", basename(caminho))
  df <- ler_excel_seguro(caminho, skip = skip)
  df <- limpar_df(df, fonte)
  if (!is.null(df)) df <- converter_datas(df)
  df
}


# --- 4. LEITURA E LIMPEZA POR GRUPO ------------------------------------------

message("\n=== GRUPO 1: Preços de Commodities (CEPEA) ===")
lst_precos <- list(
  ler_excel_seguro(resolver_arquivo(arquivos$boi_gordo))     %>% limpar_df("CEPEA_BoiGordo"),
  ler_excel_seguro(resolver_arquivo(arquivos$etanol))        %>% limpar_df("CEPEA_Etanol"),
  ler_excel_seguro(resolver_arquivo(arquivos$milho))         %>% limpar_df("CEPEA_Milho"),
  ler_excel_seguro(resolver_arquivo(arquivos$soja_paranagua))%>% limpar_df("CEPEA_SojaParanagua"),
  ler_excel_seguro(resolver_arquivo(arquivos$suino))         %>% limpar_df("CEPEA_Suino"),
  ler_excel_seguro(resolver_arquivo(arquivos$acucar))        %>% limpar_df("CEPEA_Acucar"),
  ler_excel_seguro(resolver_arquivo(arquivos$cafe_arabica))  %>% limpar_df("CEPEA_CafeArabica"),
  ler_excel_seguro(resolver_arquivo(arquivos$cafe_robusta))  %>% limpar_df("CEPEA_CafeRobusta"),
  ler_excel_seguro(resolver_arquivo(arquivos$laranja))       %>% limpar_df("Laranja_PrecosMedios"),
  ler_excel_seguro(resolver_arquivo(arquivos$manga))         %>% limpar_df("Manga_PrecosMedios")
)
df_precos <- bind_rows(Filter(Negate(is.null), lst_precos)) %>% converter_datas()
message("  Linhas consolidadas em Precos_Commodities: ", nrow(df_precos))


message("\n=== GRUPO 2: Exportações ===")
# TOTAIS_MENSAL pode ter abas de exp e imp — lemos tudo e filtramos depois
df_totais_raw <- ler_todas_abas(resolver_arquivo(arquivos$totais), "TOTAIS_MENSAL")

lst_exp <- list(
  ler_excel_seguro(resolver_arquivo(arquivos$exp_isic))  %>% limpar_df("EXP_ISIC_MENSAL"),
  ler_excel_seguro(resolver_arquivo(arquivos$exp_cafe))  %>% limpar_df("EXP_MENSAL_Cafe"),
  ler_excel_seguro(resolver_arquivo(arquivos$exp_milho)) %>% limpar_df("EXP_MENSAL_Milho"),
  ler_excel_seguro(resolver_arquivo(arquivos$exp_soja))  %>% limpar_df("EXP_MENSAL_Soja")
)
# Inclui as abas de exportação do TOTAIS_MENSAL (abas cujo nome contém "exp")
if (!is.null(df_totais_raw)) {
  abas_exp <- filter(df_totais_raw, str_detect(tolower(aba_origem), "exp"))
  if (nrow(abas_exp) > 0) lst_exp <- c(lst_exp, list(limpar_df(abas_exp, "TOTAIS_Exp")))
}
df_exportacoes <- bind_rows(Filter(Negate(is.null), lst_exp)) %>% converter_datas()
message("  Linhas consolidadas em Exportacoes: ", nrow(df_exportacoes))


message("\n=== GRUPO 3: Importações ===")
lst_imp <- list(
  ler_excel_seguro(resolver_arquivo(arquivos$imp_isic)) %>% limpar_df("IMP_ISIC_MENSAL")
)
if (!is.null(df_totais_raw)) {
  abas_imp <- filter(df_totais_raw, str_detect(tolower(aba_origem), "imp"))
  if (nrow(abas_imp) > 0) lst_imp <- c(lst_imp, list(limpar_df(abas_imp, "TOTAIS_Imp")))
}
df_importacoes <- bind_rows(Filter(Negate(is.null), lst_imp)) %>% converter_datas()
message("  Linhas consolidadas em Importacoes: ", nrow(df_importacoes))


message("\n=== GRUPO 4: PIB, Emprego e Cadeia da Soja ===")
# Este arquivo costuma ter múltiplas abas — lemos todas e empilhamos
caminho_pib <- resolver_arquivo(arquivos$pib_soja)
df_pib_soja <- if (!is.null(caminho_pib)) {
  ler_todas_abas(caminho_pib, "PIB_Soja") %>% limpar_df() %>% converter_datas()
} else NULL
message("  Linhas consolidadas em PIB_Emprego_Soja: ", if (is.null(df_pib_soja)) 0 else nrow(df_pib_soja))


# --- 5. EXPORTAÇÃO PARA EXCEL ------------------------------------------------

SAIDA <- file.path(PASTA, "Dados_Consolidados.xlsx")
message("\n=== Escrevendo arquivo de saída: ", SAIDA, " ===")

wb <- createWorkbook()

# Estilo de cabeçalho
estilo_header <- createStyle(
  fontName   = "Arial",
  fontSize   = 11,
  fontColour = "white",
  fgFill     = "#2E5FA3",
  halign     = "center",
  textDecoration = "bold",
  border     = "Bottom"
)
estilo_corpo <- createStyle(fontName = "Arial", fontSize = 10)
estilo_data  <- createStyle(fontName = "Arial", fontSize = 10,
                            numFmt = "DD/MM/YYYY")

# Função auxiliar para adicionar uma planilha formatada
adicionar_planilha <- function(wb, nome, df) {
  if (is.null(df) || nrow(df) == 0) {
    message("  [SKIP] Planilha '", nome, "' sem dados — não criada.")
    return(invisible(NULL))
  }
  addWorksheet(wb, nome)
  writeData(wb, nome, df, startRow = 1, startCol = 1, headerStyle = estilo_header)
  addStyle(wb, nome, estilo_corpo,
           rows = 2:(nrow(df) + 1), cols = 1:ncol(df), gridExpand = TRUE)
  # Formatação de data para colunas detectadas
  for (j in seq_along(df)) {
    if (inherits(df[[j]], c("POSIXct", "POSIXlt", "Date"))) {
      addStyle(wb, nome, estilo_data,
               rows = 2:(nrow(df) + 1), cols = j, gridExpand = TRUE)
    }
  }
  # Auto-largura das colunas
  setColWidths(wb, nome, cols = 1:ncol(df), widths = "auto")
  # Congela a linha de cabeçalho
  freezePane(wb, nome, firstRow = TRUE)
  message("  Planilha '", nome, "' criada com ", nrow(df), " linhas e ", ncol(df), " colunas.")
}

adicionar_planilha(wb, "Precos_Commodities",  df_precos)
adicionar_planilha(wb, "Exportacoes",         df_exportacoes)
adicionar_planilha(wb, "Importacoes",         df_importacoes)
adicionar_planilha(wb, "PIB_Emprego_Soja",    df_pib_soja)

saveWorkbook(wb, SAIDA, overwrite = TRUE)
message("\n✅ Arquivo salvo em: ", SAIDA)


# --- 6. SUMÁRIO FINAL --------------------------------------------------------

cat("\n========== SUMÁRIO ==========\n")
cat(sprintf("  %-28s %s linhas\n", "Precos_Commodities:",  nrow(df_precos)))
cat(sprintf("  %-28s %s linhas\n", "Exportacoes:",         nrow(df_exportacoes)))
cat(sprintf("  %-28s %s linhas\n", "Importacoes:",         nrow(df_importacoes)))
cat(sprintf("  %-28s %s linhas\n", "PIB_Emprego_Soja:",
            if (is.null(df_pib_soja)) 0 else nrow(df_pib_soja)))
cat("=============================\n")
cat("Arquivo de saída:", SAIDA, "\n")
