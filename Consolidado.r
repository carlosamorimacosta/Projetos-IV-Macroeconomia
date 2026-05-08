# =============================================================================
# SCRIPT COMPLETO:
# Consolidação de Preços, Exportações, Importações, Commodities e Siderurgia
#
# O que o script faz:
#   1) Lê todos os arquivos Excel e CSV indicados
#   2) Consolida:
#        - preços CEPEA e outros preços
#        - comércio exterior mensal/anual/acumulado
#        - exportações por região/país
#        - exportações por commodity
#        - importações ISIC
#        - cadeia da soja/biodiesel
#        - siderurgia em CSV
#   3) Extrai séries para:
#        - soja
#        - café
#        - milho
#        - açúcar
#        - siderurgia
#   4) Calcula:
#        - volume exportado
#        - valor FOB
#        - preço implícito
#        - participação relativa nas exportações
#   5) Gera:
#        - Excel consolidado organizado
#        - gráficos completos: 2016 até data final
#        - gráficos de curto prazo: jan/2025 até data final
#        - arquivo .tex para inclusão automática no LaTeX
# =============================================================================


# =============================================================================
# 1. PACOTES
# =============================================================================

pacotes <- c(
  "readxl",
  "openxlsx",
  "dplyr",
  "tidyr",
  "stringr",
  "purrr",
  "lubridate",
  "readr",
  "janitor",
  "ggplot2",
  "scales",
  "forcats",
  "glue",
  "stringi"
)

instalar <- pacotes[!pacotes %in% installed.packages()[, "Package"]]

if (length(instalar) > 0) {
  install.packages(instalar, dependencies = TRUE)
}

invisible(lapply(pacotes, library, character.only = TRUE))


# =============================================================================
# 2. CAMINHOS
# =============================================================================

PASTA <- "C:/Users/carlo/Downloads/3º ano - EESP/Projetos IV/Base de dados - aula 3"

SAIDA_EXCEL <- file.path(
  PASTA,
  "Base Consolidada - Commodities e Siderurgia.xlsx"
)

PASTA_GRAFICOS <- file.path(PASTA, "graficos_latex")

if (!dir.exists(PASTA_GRAFICOS)) {
  dir.create(PASTA_GRAFICOS, recursive = TRUE)
}

SAIDA_TEX <- file.path(PASTA_GRAFICOS, "figuras_exportacoes.tex")


# =============================================================================
# 3. LISTA COMPLETA DE ARQUIVOS
# =============================================================================

arquivos <- list(
  # ---------------------------------------------------------------------------
  # PREÇOS CEPEA E OUTROS PREÇOS
  # ---------------------------------------------------------------------------
  boi_gordo = c(
    file.path(PASTA, "CEPEA_BoiGordo.xls"),
    file.path(PASTA, "CEPEA_BoiGordo.xlsx")
  ),
  
  etanol = c(
    file.path(PASTA, "CEPEA_Etanol.xls"),
    file.path(PASTA, "CEPEA_Etanol.xlsx")
  ),
  
  milho_preco = c(
    file.path(PASTA, "CEPEA_Milho.xls"),
    file.path(PASTA, "CEPEA_Milho.xlsx")
  ),
  
  soja_paranagua = c(
    file.path(PASTA, "CEPEA_SojaParanagua.xls"),
    file.path(PASTA, "CEPEA_SojaParanagua.xlsx")
  ),
  
  suino = c(
    file.path(PASTA, "CEPEA_Suino.xls"),
    file.path(PASTA, "CEPEA_Suino.xlsx")
  ),
  
  acucar_preco = c(
    file.path(PASTA, "CEPEA_açucar.xls"),
    file.path(PASTA, "CEPEA_açucar.xlsx"),
    file.path(PASTA, "CEPEA_acucar.xls"),
    file.path(PASTA, "CEPEA_acucar.xlsx")
  ),
  
  cafe_arabica = c(
    file.path(PASTA, "CEPEA_cafearabica.xls"),
    file.path(PASTA, "CEPEA_cafearabica.xlsx")
  ),
  
  cafe_robusta = c(
    file.path(PASTA, "CEPEA_caferobusta.xls"),
    file.path(PASTA, "CEPEA_caferobusta.xlsx")
  ),
  
  laranja = c(
    file.path(PASTA, "laranja-precos-medios.xlsx")
  ),
  
  manga = c(
    file.path(PASTA, "manga-precos-medios.xlsx")
  ),
  
  
  # ---------------------------------------------------------------------------
  # TOTAIS DE COMÉRCIO EXTERIOR
  # ---------------------------------------------------------------------------
  totais_mensal = c(
    file.path(PASTA, "TOTAIS_MENSAL.xlsx")
  ),
  
  totais_anual = c(
    file.path(PASTA, "TOTAIS_ANUAL.xlsx")
  ),
  
  totais_acumulado = c(
    file.path(PASTA, "TOTAIS_ACUMULADO.xlsx")
  ),
  
  
  # ---------------------------------------------------------------------------
  # EXPORTAÇÕES MENSAIS POR REGIÃO / PAÍS
  # ---------------------------------------------------------------------------
  exp_america_norte = c(
    file.path(PASTA, "EXP_MENSAL_AMÉRICA DO NORTE.xlsx")
  ),
  
  exp_asia = c(
    file.path(PASTA, "EXP_MENSAL_ÁSIA (EXCLUSIVE ORIENTE MÉDIO).xlsx")
  ),
  
  exp_uniao_europeia = c(
    file.path(PASTA, "EXP_MENSAL_UNIÃO EUROPEIA - UE.xlsx")
  ),
  
  exp_paises_baixos = c(
    file.path(PASTA, "EXP_MENSAL_PAÍSES BAIXOS (HOLANDA).xlsx")
  ),
  
  exp_estados_unidos = c(
    file.path(PASTA, "EXP_MENSAL_ESTADOS UNIDOS.xlsx")
  ),
  
  exp_china = c(
    file.path(PASTA, "EXP_MENSAL_CHINA.xlsx")
  ),
  
  
  # ---------------------------------------------------------------------------
  # EXPORTAÇÕES MENSAIS POR PRODUTO / COMMODITY
  # ---------------------------------------------------------------------------
  exp_isic = c(
    file.path(PASTA, "EXP_ISIC_MENSAL.xlsx")
  ),
  
  exp_cafe = c(
    file.path(PASTA, "EXP_MENSAL_Café.xlsx"),
    file.path(PASTA, "EXP_MENSAL_Cafe.xlsx")
  ),
  
  exp_milho = c(
    file.path(PASTA, "EXP_MENSAL_Milho.xlsx")
  ),
  
  exp_soja = c(
    file.path(PASTA, "EXP_MENSAL_Soja.xlsx")
  ),
  
  exp_acucar = c(
    file.path(PASTA, "EXP_MENSAL_Açúcar.xlsx"),
    file.path(PASTA, "EXP_MENSAL_Acucar.xlsx")
  ),
  
  
  # ---------------------------------------------------------------------------
  # IMPORTAÇÕES
  # ---------------------------------------------------------------------------
  imp_isic = c(
    file.path(PASTA, "IMP_ISIC_MENSAL.xlsx")
  ),
  
  
  # ---------------------------------------------------------------------------
  # PIB, EMPREGO E COMÉRCIO EXTERIOR — CADEIA DA SOJA / BIODIESEL
  # ---------------------------------------------------------------------------
  pib_soja = c(
    file.path(
      PASTA,
      "Dados PIB, emprego e comércio exterior - cadeia da soja e do biodiesel - ENVIO (5).xlsx"
    )
  ),
  
  
  # ---------------------------------------------------------------------------
  # SIDERURGIA — CSVs DO CÓDIGO ORIGINAL
  # ---------------------------------------------------------------------------
  dados_gerais_siderurgia = c(
    file.path(PASTA, "dados_gerais.csv")
  ),
  
  dados_atuais_siderurgia = c(
    file.path(PASTA, "dados_atuais.csv")
  )
)


# =============================================================================
# 4. CLASSIFICAÇÃO DOS GRUPOS DE ARQUIVOS
# =============================================================================

chaves_precos <- c(
  "boi_gordo",
  "etanol",
  "milho_preco",
  "soja_paranagua",
  "suino",
  "acucar_preco",
  "cafe_arabica",
  "cafe_robusta",
  "laranja",
  "manga"
)

chaves_comex <- c(
  "totais_mensal",
  "totais_anual",
  "totais_acumulado",
  "exp_america_norte",
  "exp_asia",
  "exp_uniao_europeia",
  "exp_paises_baixos",
  "exp_estados_unidos",
  "exp_china",
  "exp_isic",
  "exp_cafe",
  "exp_milho",
  "exp_soja",
  "exp_acucar",
  "imp_isic"
)

chaves_pib_soja <- c(
  "pib_soja"
)

chaves_siderurgia_csv <- c(
  "dados_gerais_siderurgia",
  "dados_atuais_siderurgia"
)


# =============================================================================
# 5. FUNÇÕES AUXILIARES GERAIS
# =============================================================================

resolver_arquivo <- function(caminhos) {
  caminhos <- unlist(caminhos)
  existentes <- caminhos[file.exists(caminhos)]
  
  if (length(existentes) == 0) {
    return(NULL)
  }
  
  existentes[1]
}


padronizar_texto <- function(x) {
  x %>%
    as.character() %>%
    stringi::stri_trans_general("Latin-ASCII") %>%
    stringr::str_to_lower() %>%
    stringr::str_squish()
}


parse_numero_br <- function(x) {
  if (is.numeric(x)) return(x)
  
  x_chr <- as.character(x)
  x_chr <- stringr::str_squish(x_chr)
  x_chr[x_chr %in% c("", "NA", "NaN", "-", "--")] <- NA_character_
  
  tem_virgula <- stringr::str_detect(x_chr, ",")
  
  out <- rep(NA_real_, length(x_chr))
  
  if (any(tem_virgula, na.rm = TRUE)) {
    out[tem_virgula] <- readr::parse_number(
      x_chr[tem_virgula],
      locale = readr::locale(decimal_mark = ",", grouping_mark = ".")
    )
  }
  
  if (any(!tem_virgula, na.rm = TRUE)) {
    out[!tem_virgula] <- readr::parse_number(
      x_chr[!tem_virgula],
      locale = readr::locale(decimal_mark = ".", grouping_mark = ",")
    )
  }
  
  out
}


primeira_coluna_existente <- function(df, candidatos) {
  candidatos <- candidatos[candidatos %in% names(df)]
  
  if (length(candidatos) == 0) {
    return(NA_character_)
  }
  
  candidatos[1]
}


limpar_nome_planilha <- function(x) {
  x %>%
    stringi::stri_trans_general("Latin-ASCII") %>%
    stringr::str_replace_all("[^A-Za-z0-9_]", "_") %>%
    stringr::str_replace_all("_+", "_") %>%
    stringr::str_replace_all("^_|_$", "") %>%
    substr(1, 31)
}


# =============================================================================
# 6. FUNÇÕES DE LEITURA
# =============================================================================

ler_excel_todas_abas <- function(caminho, nome_fonte) {
  
  if (is.null(caminho) || !file.exists(caminho)) {
    message("[AVISO] Arquivo não encontrado: ", nome_fonte)
    return(NULL)
  }
  
  abas <- tryCatch(
    readxl::excel_sheets(caminho),
    error = function(e) {
      message("[ERRO] Não foi possível listar abas de: ", basename(caminho))
      return(NULL)
    }
  )
  
  if (is.null(abas)) return(NULL)
  
  lista <- purrr::map(abas, function(aba) {
    
    df <- tryCatch(
      readxl::read_excel(caminho, sheet = aba, col_types = "text"),
      error = function(e) {
        message("[ERRO] Falha ao ler: ", basename(caminho), " | aba: ", aba)
        return(NULL)
      }
    )
    
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    df %>%
      janitor::clean_names() %>%
      mutate(
        fonte_arquivo = basename(caminho),
        fonte_grupo = nome_fonte,
        aba_origem = aba,
        .before = 1
      )
  })
  
  bind_rows(Filter(Negate(is.null), lista))
}


ler_grupo_excel <- function(chaves, nome_grupo) {
  
  message("\n==================== LENDO GRUPO: ", nome_grupo, " ====================")
  
  bases <- purrr::map(chaves, function(chave) {
    
    caminho_resolvido <- resolver_arquivo(arquivos[[chave]])
    
    if (is.null(caminho_resolvido)) {
      message("[AVISO] Arquivo não encontrado para: ", chave)
      return(NULL)
    }
    
    if (stringr::str_detect(tolower(caminho_resolvido), "\\.csv$")) {
      message("[INFO] Ignorando CSV nesta etapa: ", basename(caminho_resolvido))
      return(NULL)
    }
    
    message("[OK] Lendo: ", basename(caminho_resolvido), " | chave: ", chave)
    
    ler_excel_todas_abas(caminho_resolvido, chave)
  })
  
  bind_rows(Filter(Negate(is.null), bases))
}


limpar_csv_siderurgia <- function(caminho, tipo_serie) {
  
  if (is.null(caminho) || !file.exists(caminho)) {
    message("[AVISO] CSV de siderurgia não encontrado: ", tipo_serie)
    return(NULL)
  }
  
  message("[OK] Lendo CSV siderurgia: ", basename(caminho), " | ", tipo_serie)
  
  df <- tryCatch(
    readr::read_csv(
      caminho,
      locale = readr::locale(encoding = "UTF-8"),
      col_types = cols(.default = col_character()),
      show_col_types = FALSE
    ),
    error = function(e) {
      tryCatch(
        readr::read_csv(
          caminho,
          locale = readr::locale(encoding = "Latin1"),
          col_types = cols(.default = col_character()),
          show_col_types = FALSE
        ),
        error = function(e2) {
          message("[ERRO] Não foi possível ler CSV: ", basename(caminho))
          return(NULL)
        }
      )
    }
  )
  
  if (is.null(df) || nrow(df) == 0) return(NULL)
  
  names(df)[1] <- stringr::str_replace(names(df)[1], "\uFEFF", "")
  
  df %>%
    mutate(across(where(is.character), ~ stringr::str_replace_all(.x, "\r", ""))) %>%
    janitor::clean_names() %>%
    mutate(
      fonte_arquivo = basename(caminho),
      fonte_grupo = tipo_serie,
      aba_origem = "csv",
      .before = 1
    )
}


ler_siderurgia_csv <- function() {
  
  message("\n==================== LENDO CSVs DE SIDERURGIA ====================")
  
  df_sid_gerais <- limpar_csv_siderurgia(
    resolver_arquivo(arquivos$dados_gerais_siderurgia),
    "siderurgia_anual"
  )
  
  df_sid_atuais <- limpar_csv_siderurgia(
    resolver_arquivo(arquivos$dados_atuais_siderurgia),
    "siderurgia_mensal"
  )
  
  bind_rows(df_sid_gerais, df_sid_atuais)
}


# =============================================================================
# 7. FUNÇÕES PARA DATAS
# =============================================================================

criar_data_mensal <- function(df) {
  
  col_data <- primeira_coluna_existente(
    df,
    c(
      "data",
      "date",
      "periodo",
      "periodo_ano_mes",
      "mes_ano",
      "ano_mes",
      "co_ano_mes"
    )
  )
  
  col_ano <- primeira_coluna_existente(
    df,
    c("ano", "year", "co_ano")
  )
  
  col_mes <- primeira_coluna_existente(
    df,
    c("mes", "m_s", "month", "co_mes", "nome_mes")
  )
  
  if (!is.na(col_data)) {
    data <- suppressWarnings(
      lubridate::parse_date_time(
        df[[col_data]],
        orders = c("ymd", "dmy", "mdy", "ym", "my", "Y", "Y-m", "m/Y", "Y/m")
      )
    )
    
    return(as.Date(lubridate::floor_date(data, "month")))
  }
  
  if (!is.na(col_ano) && !is.na(col_mes)) {
    
    ano <- suppressWarnings(as.integer(parse_numero_br(df[[col_ano]])))
    mes_raw <- df[[col_mes]]
    mes <- suppressWarnings(as.integer(parse_numero_br(mes_raw)))
    
    if (all(is.na(mes))) {
      mes_txt <- padronizar_texto(mes_raw)
      
      mes <- dplyr::case_when(
        stringr::str_detect(mes_txt, "jan") ~ 1L,
        stringr::str_detect(mes_txt, "fev|feb") ~ 2L,
        stringr::str_detect(mes_txt, "mar") ~ 3L,
        stringr::str_detect(mes_txt, "abr|apr") ~ 4L,
        stringr::str_detect(mes_txt, "mai|may") ~ 5L,
        stringr::str_detect(mes_txt, "jun") ~ 6L,
        stringr::str_detect(mes_txt, "jul") ~ 7L,
        stringr::str_detect(mes_txt, "ago|aug") ~ 8L,
        stringr::str_detect(mes_txt, "set|sep") ~ 9L,
        stringr::str_detect(mes_txt, "out|oct") ~ 10L,
        stringr::str_detect(mes_txt, "nov") ~ 11L,
        stringr::str_detect(mes_txt, "dez|dec") ~ 12L,
        TRUE ~ NA_integer_
      )
    }
    
    return(as.Date(sprintf("%04d-%02d-01", ano, mes)))
  }
  
  return(as.Date(rep(NA, nrow(df))))
}


criar_data_anual <- function(df) {
  
  col_ano <- primeira_coluna_existente(
    df,
    c("ano", "year", "co_ano")
  )
  
  if (is.na(col_ano)) {
    return(as.Date(rep(NA, nrow(df))))
  }
  
  ano <- suppressWarnings(as.integer(parse_numero_br(df[[col_ano]])))
  
  as.Date(sprintf("%04d-01-01", ano))
}


# =============================================================================
# 8. PADRONIZAÇÃO DE COMÉRCIO EXTERIOR
# =============================================================================

extrair_colunas_comex <- function(df) {
  
  data_extraida <- criar_data_mensal(df)
  
  col_valor <- primeira_coluna_existente(
    df,
    c(
      "valor_fob_us",
      "valor_fob_usd",
      "valor_fob",
      "vl_fob",
      "vl_fob_us",
      "vl_fob_usd",
      "valor_us",
      "valor_usd",
      "us_fob",
      "fob",
      "vl_fob_dolar",
      "vl_fob_us_dolar"
    )
  )
  
  col_kg <- primeira_coluna_existente(
    df,
    c(
      "quilograma_liquido",
      "quilograma_liquido_kg",
      "kg_liquido",
      "kg",
      "peso_liquido",
      "vl_kg_liquido",
      "net_weight",
      "net_weight_kg"
    )
  )
  
  col_qtd <- primeira_coluna_existente(
    df,
    c(
      "quantidade_estatistica",
      "qt_estatistica",
      "quantidade",
      "qtd",
      "quantity",
      "vl_quantidade_estatistica"
    )
  )
  
  col_ncm <- primeira_coluna_existente(
    df,
    c(
      "codigo_ncm",
      "co_ncm",
      "ncm",
      "cod_ncm",
      "sh4",
      "sh6",
      "sh2",
      "codigo_sh",
      "co_sh",
      "codigo"
    )
  )
  
  col_desc <- primeira_coluna_existente(
    df,
    c(
      "descricao_ncm",
      "no_ncm_por",
      "desc_ncm",
      "produto",
      "descricao",
      "descricao_do_produto",
      "mercadoria",
      "setor",
      "atividade",
      "isic",
      "descricao_isic",
      "nome_produto",
      "no_sh6_por",
      "no_sh4_por",
      "no_sh2_por"
    )
  )
  
  col_pais <- primeira_coluna_existente(
    df,
    c(
      "pais",
      "pais_destino",
      "nome_pais",
      "no_pais",
      "country",
      "destino",
      "mercado"
    )
  )
  
  col_bloco <- primeira_coluna_existente(
    df,
    c(
      "bloco",
      "regiao",
      "regiao_destino",
      "regiao_geografica",
      "fonte_grupo"
    )
  )
  
  df %>%
    mutate(
      data = data_extraida,
      ano = lubridate::year(data),
      mes = lubridate::month(data),
      
      ncm_codigo = if (!is.na(col_ncm)) as.character(.data[[col_ncm]]) else NA_character_,
      
      descricao_produto = if (!is.na(col_desc)) {
        as.character(.data[[col_desc]])
      } else {
        NA_character_
      },
      
      pais_destino = if (!is.na(col_pais)) {
        as.character(.data[[col_pais]])
      } else {
        NA_character_
      },
      
      regiao_destino = if (!is.na(col_bloco)) {
        as.character(.data[[col_bloco]])
      } else {
        fonte_grupo
      },
      
      valor_fob_usd = if (!is.na(col_valor)) {
        parse_numero_br(.data[[col_valor]])
      } else {
        NA_real_
      },
      
      volume_kg = if (!is.na(col_kg)) {
        parse_numero_br(.data[[col_kg]])
      } else {
        NA_real_
      },
      
      quantidade_estatistica = if (!is.na(col_qtd)) {
        parse_numero_br(.data[[col_qtd]])
      } else {
        NA_real_
      }
    ) %>%
    mutate(
      ncm_codigo = stringr::str_replace_all(ncm_codigo, "\\D", ""),
      descricao_produto_limpa = padronizar_texto(descricao_produto),
      pais_destino = stringr::str_squish(pais_destino),
      regiao_destino = stringr::str_squish(regiao_destino)
    )
}


classificar_commodity <- function(ncm, desc, fonte_grupo, fonte_arquivo) {
  
  ncm <- ifelse(is.na(ncm), "", ncm)
  desc <- ifelse(is.na(desc), "", desc)
  
  fonte <- padronizar_texto(paste(fonte_grupo, fonte_arquivo))
  
  dplyr::case_when(
    stringr::str_detect(fonte, "cafe") |
      stringr::str_detect(desc, "cafe|coffee") |
      stringr::str_detect(ncm, "^0901") ~ "Café",
    
    stringr::str_detect(fonte, "milho") |
      stringr::str_detect(desc, "milho|corn|maize") |
      stringr::str_detect(ncm, "^1005") ~ "Milho",
    
    stringr::str_detect(fonte, "soja") |
      stringr::str_detect(desc, "soja|soy|soybean|farelo de soja|oleo de soja|soybeans") |
      stringr::str_detect(ncm, "^1201|^1507|^2304") ~ "Soja",
    
    stringr::str_detect(fonte, "acucar|açucar|açúcar") |
      stringr::str_detect(desc, "acucar|açucar|sugar|cana de acucar|cana-de-acucar") |
      stringr::str_detect(ncm, "^1701|^1702") ~ "Açúcar",
    
    stringr::str_detect(desc, "ferro|aco|aço|siderurg|steel|iron|fundido") |
      stringr::str_detect(ncm, "^72|^73") ~ "Siderurgia",
    
    TRUE ~ "Outros"
  )
}


classificar_subcategoria <- function(commodity, ncm, desc) {
  
  ncm <- ifelse(is.na(ncm), "", ncm)
  desc <- ifelse(is.na(desc), "", desc)
  
  dplyr::case_when(
    commodity == "Soja" & stringr::str_detect(ncm, "^1201") ~ "Soja em grão",
    commodity == "Soja" & stringr::str_detect(ncm, "^1507") ~ "Óleo de soja",
    commodity == "Soja" & stringr::str_detect(ncm, "^2304") ~ "Farelo de soja",
    commodity == "Soja" ~ "Soja - outros",
    
    commodity == "Café" & stringr::str_detect(ncm, "^090111|^090112") ~ "Café não torrado",
    commodity == "Café" & stringr::str_detect(ncm, "^090121|^090122") ~ "Café torrado",
    commodity == "Café" & stringr::str_detect(ncm, "^090190") ~ "Outros produtos de café",
    commodity == "Café" ~ "Café - outros",
    
    commodity == "Milho" & stringr::str_detect(ncm, "^1005") ~ "Milho em grão",
    commodity == "Milho" ~ "Milho - outros",
    
    commodity == "Açúcar" & stringr::str_detect(ncm, "^1701") ~ "Açúcar de cana/beterraba",
    commodity == "Açúcar" & stringr::str_detect(ncm, "^1702") ~ "Outros açúcares",
    commodity == "Açúcar" ~ "Açúcar - outros",
    
    commodity == "Siderurgia" & stringr::str_detect(ncm, "^7201|^7202|^7203|^7204") ~ "Insumos siderúrgicos",
    commodity == "Siderurgia" & stringr::str_detect(ncm, "^7205|^7206|^7207") ~ "Semimanufaturados de ferro/aço",
    commodity == "Siderurgia" & stringr::str_detect(ncm, "^7208|^7209|^7210|^7211|^7212") ~ "Laminados planos",
    commodity == "Siderurgia" & stringr::str_detect(ncm, "^7213|^7214|^7215|^7216|^7217") ~ "Laminados longos e fios",
    commodity == "Siderurgia" & stringr::str_detect(ncm, "^722") ~ "Aços especiais",
    commodity == "Siderurgia" & stringr::str_detect(ncm, "^73") ~ "Obras de ferro/aço",
    commodity == "Siderurgia" ~ "Siderurgia - outros",
    
    TRUE ~ "Outros"
  )
}


padronizar_comex <- function(df) {
  
  if (is.null(df) || nrow(df) == 0) return(NULL)
  
  df %>%
    extrair_colunas_comex() %>%
    mutate(
      tipo_fluxo = case_when(
        stringr::str_detect(padronizar_texto(fonte_arquivo), "^imp|import") |
          stringr::str_detect(padronizar_texto(fonte_grupo), "^imp|import") ~ "Importação",
        TRUE ~ "Exportação"
      ),
      
      periodicidade = case_when(
        stringr::str_detect(padronizar_texto(fonte_arquivo), "anual") |
          stringr::str_detect(padronizar_texto(fonte_grupo), "anual") ~ "Anual",
        
        stringr::str_detect(padronizar_texto(fonte_arquivo), "acumulado") |
          stringr::str_detect(padronizar_texto(fonte_grupo), "acumulado") ~ "Acumulado",
        
        TRUE ~ "Mensal"
      ),
      
      commodity = classificar_commodity(
        ncm_codigo,
        descricao_produto_limpa,
        fonte_grupo,
        fonte_arquivo
      ),
      
      subcategoria = classificar_subcategoria(
        commodity,
        ncm_codigo,
        descricao_produto_limpa
      ),
      
      volume_ton = volume_kg / 1000,
      
      preco_implicito_usd_ton = dplyr::if_else(
        !is.na(volume_ton) & volume_ton > 0,
        valor_fob_usd / volume_ton,
        NA_real_
      )
    ) %>%
    select(
      fonte_arquivo,
      fonte_grupo,
      aba_origem,
      tipo_fluxo,
      periodicidade,
      data,
      ano,
      mes,
      commodity,
      subcategoria,
      ncm_codigo,
      descricao_produto,
      pais_destino,
      regiao_destino,
      valor_fob_usd,
      volume_kg,
      volume_ton,
      quantidade_estatistica,
      preco_implicito_usd_ton,
      everything()
    )
}


# =============================================================================
# 8.1 FUNÇÃO CORRIGIDA PARA CONVERTER NÚMEROS
# =============================================================================

parse_numero_br <- function(x) {
  if (is.numeric(x)) {
    return(x)
  }
  
  x_chr <- as.character(x)
  
  x_chr <- x_chr %>%
    stringr::str_replace_all("\u00A0", " ") %>%
    stringr::str_squish()
  
  x_chr[x_chr %in% c("", "NA", "NaN", "NULL", "-", "--", "...")] <- NA_character_
  
  out <- rep(NA_real_, length(x_chr))
  
  idx_valido <- !is.na(x_chr)
  
  if (!any(idx_valido)) {
    return(out)
  }
  
  tem_virgula <- rep(FALSE, length(x_chr))
  tem_virgula[idx_valido] <- stringr::str_detect(x_chr[idx_valido], ",")
  
  idx_br <- idx_valido & tem_virgula
  idx_us <- idx_valido & !tem_virgula
  
  if (any(idx_br)) {
    out[idx_br] <- readr::parse_number(
      x_chr[idx_br],
      locale = readr::locale(decimal_mark = ",", grouping_mark = ".")
    )
  }
  
  if (any(idx_us)) {
    out[idx_us] <- readr::parse_number(
      x_chr[idx_us],
      locale = readr::locale(decimal_mark = ".", grouping_mark = ",")
    )
  }
  
  out
}


# =============================================================================
# 9. PADRONIZAÇÃO DE PREÇOS CEPEA E OUTROS PREÇOS
# =============================================================================

padronizar_precos <- function(df) {
  
  if (is.null(df) || nrow(df) == 0) {
    return(NULL)
  }
  
  col_data <- primeira_coluna_existente(
    df,
    c(
      "data",
      "date",
      "periodo",
      "mes",
      "mes_ano",
      "ano_mes",
      "dt",
      "dia"
    )
  )
  
  col_preco <- primeira_coluna_existente(
    df,
    c(
      "preco",
      "preco_rs",
      "preco_r",
      "preco_r_",
      "valor",
      "valor_rs",
      "indicador",
      "preco_medio",
      "media",
      "r",
      "rs",
      "preco_a_vista",
      "preco_nominal"
    )
  )
  
  data_extraida <- if (!is.na(col_data)) {
    suppressWarnings(
      as.Date(
        lubridate::parse_date_time(
          df[[col_data]],
          orders = c("dmy", "ymd", "mdy", "ym", "my", "Y")
        )
      )
    )
  } else {
    criar_data_mensal(df)
  }
  
  df_saida <- df %>%
    mutate(
      data = data_extraida,
      ano = lubridate::year(data),
      mes = lubridate::month(data),
      
      preco = if (!is.na(col_preco)) {
        parse_numero_br(.data[[col_preco]])
      } else {
        NA_real_
      },
      
      produto_preco = case_when(
        stringr::str_detect(padronizar_texto(fonte_grupo), "boi") ~ "Boi gordo",
        stringr::str_detect(padronizar_texto(fonte_grupo), "etanol") ~ "Etanol",
        stringr::str_detect(padronizar_texto(fonte_grupo), "milho") ~ "Milho",
        stringr::str_detect(padronizar_texto(fonte_grupo), "soja") ~ "Soja Paranaguá",
        stringr::str_detect(padronizar_texto(fonte_grupo), "suino") ~ "Suíno",
        stringr::str_detect(padronizar_texto(fonte_grupo), "acucar") ~ "Açúcar",
        stringr::str_detect(padronizar_texto(fonte_grupo), "cafe_arabica") ~ "Café arábica",
        stringr::str_detect(padronizar_texto(fonte_grupo), "cafe_robusta") ~ "Café robusta",
        stringr::str_detect(padronizar_texto(fonte_grupo), "laranja") ~ "Laranja",
        stringr::str_detect(padronizar_texto(fonte_grupo), "manga") ~ "Manga",
        TRUE ~ fonte_grupo
      )
    ) %>%
    select(
      fonte_arquivo,
      fonte_grupo,
      aba_origem,
      produto_preco,
      data,
      ano,
      mes,
      preco,
      everything()
    )
  
  df_saida
}


# =============================================================================
# 10. LEITURA EFETIVA DOS DADOS
# =============================================================================

base_precos_raw <- ler_grupo_excel(chaves_precos, "PREÇOS")

base_comex_raw_excel <- ler_grupo_excel(chaves_comex, "COMÉRCIO EXTERIOR")

base_pib_soja_raw <- ler_grupo_excel(chaves_pib_soja, "PIB SOJA/BIODIESEL")

base_siderurgia_csv <- ler_siderurgia_csv()


# Junta comércio exterior em Excel com CSVs de siderurgia
base_comex_raw <- bind_rows(
  base_comex_raw_excel,
  base_siderurgia_csv
)


if (is.null(base_comex_raw) || nrow(base_comex_raw) == 0) {
  stop("Nenhum dado de comércio exterior foi lido. Verifique os caminhos dos arquivos.")
}


# =============================================================================
# 11. PADRONIZAÇÃO DAS BASES
# =============================================================================

message("\n==================== PADRONIZANDO BASES ====================")

base_precos <- padronizar_precos(base_precos_raw)

base_comex <- padronizar_comex(base_comex_raw)

base_pib_soja <- if (!is.null(base_pib_soja_raw) && nrow(base_pib_soja_raw) > 0) {
  base_pib_soja_raw %>%
    mutate(across(everything(), as.character))
} else {
  NULL
}

# =============================================================================
# 12. BASE DE EXPORTAÇÕES MENSAIS PARA COMMODITIES
# =============================================================================

commodities_alvo <- c(
  "Soja",
  "Café",
  "Milho",
  "Açúcar",
  "Siderurgia"
)


base_export_mensal <- base_comex %>%
  filter(
    tipo_fluxo == "Exportação",
    periodicidade == "Mensal",
    !is.na(data),
    data >= as.Date("2016-01-01")
  )


# Total mensal de exportações.
# O script usa a soma dos registros disponíveis em cada mês.
# Se a base tiver TOTAIS_MENSAL, ele entra no mesmo total.
total_export_mensal <- base_export_mensal %>%
  group_by(data) %>%
  summarise(
    total_exportado_usd = sum(valor_fob_usd, na.rm = TRUE),
    .groups = "drop"
  )


base_commodities_mensal <- base_export_mensal %>%
  filter(commodity %in% commodities_alvo) %>%
  group_by(data, ano, mes, commodity, subcategoria) %>%
  summarise(
    valor_fob_usd = sum(valor_fob_usd, na.rm = TRUE),
    volume_kg = sum(volume_kg, na.rm = TRUE),
    volume_ton = sum(volume_ton, na.rm = TRUE),
    quantidade_estatistica = sum(quantidade_estatistica, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(total_export_mensal, by = "data") %>%
  mutate(
    preco_implicito_usd_ton = if_else(
      volume_ton > 0,
      valor_fob_usd / volume_ton,
      NA_real_
    ),
    
    participacao_exportacoes = if_else(
      total_exportado_usd > 0,
      valor_fob_usd / total_exportado_usd,
      NA_real_
    )
  ) %>%
  arrange(commodity, subcategoria, data)


base_commodities_total_mensal <- base_commodities_mensal %>%
  group_by(data, ano, mes, commodity) %>%
  summarise(
    valor_fob_usd = sum(valor_fob_usd, na.rm = TRUE),
    volume_kg = sum(volume_kg, na.rm = TRUE),
    volume_ton = sum(volume_ton, na.rm = TRUE),
    total_exportado_usd = max(total_exportado_usd, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    preco_implicito_usd_ton = if_else(
      volume_ton > 0,
      valor_fob_usd / volume_ton,
      NA_real_
    ),
    
    participacao_exportacoes = if_else(
      total_exportado_usd > 0,
      valor_fob_usd / total_exportado_usd,
      NA_real_
    )
  ) %>%
  arrange(commodity, data)


# =============================================================================
# 13. RESUMOS ANALÍTICOS
# =============================================================================

resumo_commodity <- base_commodities_total_mensal %>%
  group_by(commodity) %>%
  summarise(
    data_inicial = min(data, na.rm = TRUE),
    data_final = max(data, na.rm = TRUE),
    valor_fob_total_usd = sum(valor_fob_usd, na.rm = TRUE),
    volume_total_ton = sum(volume_ton, na.rm = TRUE),
    preco_medio_implicito_usd_ton = valor_fob_total_usd / volume_total_ton,
    participacao_media = mean(participacao_exportacoes, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(valor_fob_total_usd))


resumo_subcategoria <- base_commodities_mensal %>%
  group_by(commodity, subcategoria) %>%
  summarise(
    data_inicial = min(data, na.rm = TRUE),
    data_final = max(data, na.rm = TRUE),
    valor_fob_total_usd = sum(valor_fob_usd, na.rm = TRUE),
    volume_total_ton = sum(volume_ton, na.rm = TRUE),
    preco_medio_implicito_usd_ton = valor_fob_total_usd / volume_total_ton,
    participacao_media = mean(participacao_exportacoes, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(commodity, desc(valor_fob_total_usd))


resumo_destinos <- base_export_mensal %>%
  filter(commodity %in% commodities_alvo) %>%
  group_by(commodity, pais_destino) %>%
  summarise(
    valor_fob_total_usd = sum(valor_fob_usd, na.rm = TRUE),
    volume_total_ton = sum(volume_ton, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(commodity) %>%
  mutate(
    participacao_no_total_da_commodity =
      valor_fob_total_usd / sum(valor_fob_total_usd, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  arrange(commodity, desc(valor_fob_total_usd))


resumo_arquivos <- tibble(
  chave = names(arquivos),
  caminho_resolvido = map_chr(arquivos, ~ {
    arq <- resolver_arquivo(.x)
    ifelse(is.null(arq), NA_character_, arq)
  }),
  encontrado = !is.na(caminho_resolvido)
)


# =============================================================================
# 14. GRÁFICOS PARA LATEX
# =============================================================================

tema_grafico <- theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 13),
    plot.subtitle = element_text(size = 10),
    axis.title = element_text(size = 10),
    axis.text = element_text(size = 9),
    legend.position = "bottom",
    panel.grid.minor = element_blank()
  )


salvar_grafico <- function(df, commodity_escolhida, indicador, nome_indicador, janela) {
  
  df_plot <- df %>%
    filter(commodity == commodity_escolhida) %>%
    arrange(data)
  
  if (janela == "curto") {
    df_plot <- df_plot %>% filter(data >= as.Date("2025-01-01"))
    subtitulo <- "Série de curto prazo: jan/2025 até a última data disponível"
    sufixo <- "curto_prazo"
  } else {
    df_plot <- df_plot %>% filter(data >= as.Date("2016-01-01"))
    subtitulo <- "Série completa: 2016 até a última data disponível"
    sufixo <- "serie_completa"
  }
  
  if (nrow(df_plot) == 0) {
    return(NULL)
  }
  
  y_lab <- case_when(
    indicador == "valor_fob_usd" ~ "Valor FOB, US$",
    indicador == "volume_ton" ~ "Volume exportado, toneladas",
    indicador == "preco_implicito_usd_ton" ~ "Preço implícito, US$/tonelada",
    indicador == "participacao_exportacoes" ~ "Participação nas exportações totais",
    TRUE ~ indicador
  )
  
  escala_y <- if (indicador == "participacao_exportacoes") {
    scale_y_continuous(labels = percent_format(accuracy = 0.1))
  } else {
    scale_y_continuous(labels = label_number(big.mark = ".", decimal.mark = ","))
  }
  
  g <- ggplot(df_plot, aes(x = data, y = .data[[indicador]])) +
    geom_line(linewidth = 0.8, na.rm = TRUE) +
    escala_y +
    scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
    labs(
      title = paste0(commodity_escolhida, " — ", nome_indicador),
      subtitle = subtitulo,
      x = NULL,
      y = y_lab,
      caption = "Fonte: elaboração própria a partir dos arquivos da base de dados."
    ) +
    tema_grafico
  
  nome_base <- commodity_escolhida %>%
    stringi::stri_trans_general("Latin-ASCII") %>%
    str_to_lower() %>%
    str_replace_all("[^a-z0-9]+", "_") %>%
    str_replace_all("^_|_$", "")
  
  nome_ind <- indicador %>%
    str_to_lower() %>%
    str_replace_all("[^a-z0-9]+", "_")
  
  arquivo_pdf <- file.path(
    PASTA_GRAFICOS,
    paste0(nome_base, "_", nome_ind, "_", sufixo, ".pdf")
  )
  
  arquivo_png <- file.path(
    PASTA_GRAFICOS,
    paste0(nome_base, "_", nome_ind, "_", sufixo, ".png")
  )
  
  ggsave(arquivo_pdf, g, width = 8.5, height = 5.0, device = cairo_pdf)
  ggsave(arquivo_png, g, width = 8.5, height = 5.0, dpi = 300)
  
  tibble(
    commodity = commodity_escolhida,
    indicador = indicador,
    nome_indicador = nome_indicador,
    janela = janela,
    arquivo_pdf = arquivo_pdf,
    arquivo_png = arquivo_png
  )
}


indicadores_graficos <- tribble(
  ~indicador, ~nome_indicador,
  "valor_fob_usd", "Valor FOB",
  "volume_ton", "Volume exportado",
  "preco_implicito_usd_ton", "Preço implícito",
  "participacao_exportacoes", "Participação relativa nas exportações"
)


message("\n==================== GERANDO GRÁFICOS ====================")

graficos_gerados <- pmap_dfr(
  expand_grid(
    commodity_escolhida = commodities_alvo,
    indicadores_graficos,
    janela = c("completa", "curto")
  ),
  function(commodity_escolhida, indicador, nome_indicador, janela) {
    salvar_grafico(
      df = base_commodities_total_mensal,
      commodity_escolhida = commodity_escolhida,
      indicador = indicador,
      nome_indicador = nome_indicador,
      janela = janela
    )
  }
)


# =============================================================================
# 15. ARQUIVO .TEX COM FIGURAS
# =============================================================================

gerar_bloco_latex <- function(arquivo_pdf, commodity, nome_indicador, janela) {
  
  caminho_rel <- basename(arquivo_pdf)
  
  titulo_janela <- ifelse(
    janela == "completa",
    "série completa, 2016 até a última data disponível",
    "curto prazo, janeiro de 2025 até a última data disponível"
  )
  
  label <- paste0(
    "fig:",
    commodity %>%
      stringi::stri_trans_general("Latin-ASCII") %>%
      str_to_lower() %>%
      str_replace_all("[^a-z0-9]+", "-"),
    "-",
    nome_indicador %>%
      stringi::stri_trans_general("Latin-ASCII") %>%
      str_to_lower() %>%
      str_replace_all("[^a-z0-9]+", "-"),
    "-",
    janela
  )
  
  glue(
    "\\begin{{figure}}[H]
    \\centering
    \\includegraphics[width=0.92\\textwidth]{{{caminho_rel}}}
    \\caption{{{commodity}: {nome_indicador} — {titulo_janela}.}}
    \\label{{{label}}}
\\end{{figure}}

"
  )
}


cabecalho_tex <- "
% =============================================================================
% Figuras geradas automaticamente pelo R
%
% Para usar no LaTeX:
%
% \\usepackage{graphicx}
% \\usepackage{float}
%
% Depois, no corpo do texto:
%
% \\input{figuras_exportacoes.tex}
% =============================================================================

"

texto_tex <- if (!is.null(graficos_gerados) && nrow(graficos_gerados) > 0) {
  graficos_gerados %>%
    mutate(
      bloco = pmap_chr(
        list(arquivo_pdf, commodity, nome_indicador, janela),
        gerar_bloco_latex
      )
    ) %>%
    pull(bloco) %>%
    paste(collapse = "\n")
} else {
  "% Nenhum gráfico foi gerado.\n"
}

writeLines(paste0(cabecalho_tex, texto_tex), SAIDA_TEX)


# =============================================================================
# 16. EXPORTAÇÃO PARA EXCEL FORMATADO
# =============================================================================

message("\n==================== EXPORTANDO EXCEL ====================")

wb <- createWorkbook()

estilo_titulo <- createStyle(
  fontName = "Arial",
  fontSize = 14,
  fontColour = "white",
  fgFill = "#1F4E78",
  halign = "center",
  textDecoration = "bold"
)

estilo_header <- createStyle(
  fontName = "Arial",
  fontSize = 11,
  fontColour = "white",
  fgFill = "#2F75B5",
  halign = "center",
  valign = "center",
  textDecoration = "bold",
  border = "Bottom"
)

estilo_corpo <- createStyle(
  fontName = "Arial",
  fontSize = 10,
  border = "Bottom",
  borderColour = "#D9EAF7"
)

estilo_numero <- createStyle(
  fontName = "Arial",
  fontSize = 10,
  numFmt = "#,##0.00",
  border = "Bottom",
  borderColour = "#D9EAF7"
)

estilo_inteiro <- createStyle(
  fontName = "Arial",
  fontSize = 10,
  numFmt = "#,##0",
  border = "Bottom",
  borderColour = "#D9EAF7"
)

estilo_percentual <- createStyle(
  fontName = "Arial",
  fontSize = 10,
  numFmt = "0.00%",
  border = "Bottom",
  borderColour = "#D9EAF7"
)

estilo_data <- createStyle(
  fontName = "Arial",
  fontSize = 10,
  numFmt = "DD/MM/YYYY",
  border = "Bottom",
  borderColour = "#D9EAF7"
)


adicionar_aba_formatada <- function(wb, nome, df, titulo = nome) {
  
  if (is.null(df) || nrow(df) == 0) {
    message("[SKIP] Aba sem dados: ", nome)
    return(invisible(NULL))
  }
  
  nome <- limpar_nome_planilha(nome)
  
  addWorksheet(wb, nome, gridLines = FALSE)
  
  writeData(wb, nome, titulo, startRow = 1, startCol = 1)
  mergeCells(wb, nome, cols = 1:ncol(df), rows = 1)
  addStyle(
    wb,
    nome,
    estilo_titulo,
    rows = 1,
    cols = 1:ncol(df),
    gridExpand = TRUE
  )
  
  writeData(
    wb,
    nome,
    df,
    startRow = 3,
    startCol = 1,
    headerStyle = estilo_header
  )
  
  if (nrow(df) > 0) {
    addStyle(
      wb,
      nome,
      estilo_corpo,
      rows = 4:(nrow(df) + 3),
      cols = 1:ncol(df),
      gridExpand = TRUE
    )
  }
  
  for (j in seq_along(df)) {
    
    nome_col <- names(df)[j]
    
    if (inherits(df[[j]], "Date")) {
      addStyle(
        wb,
        nome,
        estilo_data,
        rows = 4:(nrow(df) + 3),
        cols = j,
        gridExpand = TRUE,
        stack = TRUE
      )
    }
    
    if (
      is.numeric(df[[j]]) &&
      stringr::str_detect(nome_col, "participacao|share|percent|pct")
    ) {
      addStyle(
        wb,
        nome,
        estilo_percentual,
        rows = 4:(nrow(df) + 3),
        cols = j,
        gridExpand = TRUE,
        stack = TRUE
      )
    } else if (
      is.numeric(df[[j]]) &&
      stringr::str_detect(nome_col, "^ano$|^mes$")
    ) {
      addStyle(
        wb,
        nome,
        estilo_inteiro,
        rows = 4:(nrow(df) + 3),
        cols = j,
        gridExpand = TRUE,
        stack = TRUE
      )
    } else if (is.numeric(df[[j]])) {
      addStyle(
        wb,
        nome,
        estilo_numero,
        rows = 4:(nrow(df) + 3),
        cols = j,
        gridExpand = TRUE,
        stack = TRUE
      )
    }
  }
  
  freezePane(wb, nome, firstActiveRow = 4)
  addFilter(wb, nome, rows = 3, cols = 1:ncol(df))
  setColWidths(wb, nome, cols = 1:ncol(df), widths = "auto")
  
  message("Aba criada: ", nome, " | ", nrow(df), " linhas")
}


# -----------------------------------------------------------------------------
# Abas principais
# -----------------------------------------------------------------------------

adicionar_aba_formatada(
  wb,
  "README",
  tibble(
    item = c(
      "Objetivo",
      "Arquivos incluídos",
      "Commodities analisadas",
      "Período completo dos gráficos",
      "Período curto dos gráficos",
      "Indicadores calculados",
      "Observação sobre preços",
      "Observação sobre siderurgia"
    ),
    descricao = c(
      "Consolidar preços, exportações, importações, commodities e siderurgia.",
      "Todos os arquivos listados no objeto arquivos, incluindo CEPEA, COMEX, soja/biodiesel e CSVs de siderurgia.",
      "Soja, café, milho, açúcar e siderurgia.",
      "2016 até a última data disponível.",
      "Janeiro de 2025 até a última data disponível.",
      "Valor FOB, volume exportado, preço implícito e participação relativa nas exportações.",
      "Arquivos CEPEA e outros preços são mantidos em abas próprias; eles não são tratados como exportações.",
      "CSVs dados_gerais.csv e dados_atuais.csv são lidos separadamente e incorporados à base de comércio exterior."
    )
  ),
  "README da base consolidada"
)

adicionar_aba_formatada(
  wb,
  "Arquivos_Lidos",
  resumo_arquivos,
  "Resumo dos arquivos encontrados e lidos"
)

adicionar_aba_formatada(
  wb,
  "Precos_Bruta",
  base_precos_raw,
  "Base bruta de preços"
)

adicionar_aba_formatada(
  wb,
  "Precos_Padronizada",
  base_precos,
  "Base de preços padronizada"
)

adicionar_aba_formatada(
  wb,
  "Comex_Bruta",
  base_comex_raw,
  "Base bruta de comércio exterior"
)

adicionar_aba_formatada(
  wb,
  "Comex_Consolidada",
  base_comex,
  "Base consolidada de comércio exterior"
)

adicionar_aba_formatada(
  wb,
  "Commodities_Mensal",
  base_commodities_mensal,
  "Commodities mensais por subcategoria"
)

adicionar_aba_formatada(
  wb,
  "Commodities_Total",
  base_commodities_total_mensal,
  "Commodities mensais agregadas"
)

adicionar_aba_formatada(
  wb,
  "Resumo_Commodity",
  resumo_commodity,
  "Resumo por commodity"
)

adicionar_aba_formatada(
  wb,
  "Resumo_Subcategoria",
  resumo_subcategoria,
  "Resumo por commodity e subcategoria"
)

adicionar_aba_formatada(
  wb,
  "Resumo_Destinos",
  resumo_destinos,
  "Resumo por destino"
)

adicionar_aba_formatada(
  wb,
  "PIB_Soja_Biodiesel",
  base_pib_soja,
  "Base bruta da cadeia da soja e biodiesel"
)

adicionar_aba_formatada(
  wb,
  "Graficos_Gerados",
  graficos_gerados,
  "Lista de gráficos gerados para LaTeX"
)


# -----------------------------------------------------------------------------
# Abas separadas por commodity
# -----------------------------------------------------------------------------

for (cmd in commodities_alvo) {
  
  df_cmd <- base_commodities_mensal %>%
    filter(commodity == cmd)
  
  nome_aba <- paste0("Serie_", cmd)
  
  adicionar_aba_formatada(
    wb,
    nome_aba,
    df_cmd,
    paste0("Série mensal — ", cmd)
  )
}


# -----------------------------------------------------------------------------
# Abas separadas por arquivo de origem de comércio exterior
# -----------------------------------------------------------------------------

arquivos_origem_comex <- base_comex %>%
  distinct(fonte_grupo) %>%
  pull(fonte_grupo)

for (fg in arquivos_origem_comex) {
  
  df_fg <- base_comex %>%
    filter(fonte_grupo == fg)
  
  nome_aba <- paste0("Comex_", fg)
  
  adicionar_aba_formatada(
    wb,
    nome_aba,
    df_fg,
    paste0("Comércio exterior — ", fg)
  )
}


saveWorkbook(wb, SAIDA_EXCEL, overwrite = TRUE)


# =============================================================================
# 17. SUMÁRIO FINAL
# =============================================================================

cat("\n============================================================\n")
cat("PROCESSO FINALIZADO\n")
cat("============================================================\n\n")

cat("Excel consolidado salvo em:\n")
cat(SAIDA_EXCEL, "\n\n")

cat("Pasta de gráficos salva em:\n")
cat(PASTA_GRAFICOS, "\n\n")

cat("Arquivo LaTeX salvo em:\n")
cat(SAIDA_TEX, "\n\n")

cat("Resumo:\n")
cat("  Arquivos listados:             ", length(arquivos), "\n")
cat("  Arquivos encontrados:          ", sum(resumo_arquivos$encontrado), "\n")
cat("  Linhas preços brutos:          ", ifelse(is.null(base_precos_raw), 0, nrow(base_precos_raw)), "\n")
cat("  Linhas preços padronizados:    ", ifelse(is.null(base_precos), 0, nrow(base_precos)), "\n")
cat("  Linhas comex bruta:            ", nrow(base_comex_raw), "\n")
cat("  Linhas comex consolidada:      ", nrow(base_comex), "\n")
cat("  Linhas commodities mensais:    ", nrow(base_commodities_mensal), "\n")
cat("  Gráficos gerados:              ", ifelse(is.null(graficos_gerados), 0, nrow(graficos_gerados)), "\n")

cat("\n============================================================\n")
