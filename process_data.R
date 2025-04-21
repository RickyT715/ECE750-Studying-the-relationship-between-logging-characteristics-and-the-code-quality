#!/usr/bin/env Rscript
# process_data.R

# ——————————————————————————————————————————————————————————————
# 0) require jsonlite
if (!requireNamespace("jsonlite", quietly=TRUE)) {
  stop("Please install the jsonlite package: install.packages('jsonlite')")
}
library(jsonlite)

# ——————————————————————————————————————————————————————————————
# 1) parse args
args <- commandArgs(trailingOnly=TRUE)
if (length(args) != 1) {
  cat("Usage: Rscript process_data.R <metrics.csv>\n")
  quit(status=1)
}
csvfile <- args[1]

# ——————————————————————————————————————————————————————————————
# 2) read CSV
df <- read.csv(csvfile, stringsAsFactors=FALSE)

# ——————————————————————————————————————————————————————————————
# 3) clean & coerce all truly numeric columns
num_cols <- c(
  "LOC", "CODE_CHURN", "NUM_LOGS",
  "LOGADD","LOGDEL","FCOC","LOG_CODE_OWNERSHIP",
  "TPC","LOGD","LEVELD","AVG_LOG_LEN","LOG_SPREAD",
  "pre_defect_count","post_defect_count"
)
for (col in num_cols) {
  df[[col]] <- as.numeric(gsub("[^0-9\\.\\-]", "", df[[col]]))
}

# ——————————————————————————————————————————————————————————————
# 4) compute post‑release defect density per KLOC
df$post_defect_density <- with(df,
  ifelse(LOC > 0, post_defect_count * 1000 / LOC, NA)
)

# ——————————————————————————————————————————————————————————————
# 5) basic totals & percentages
lines_of_code      <- sum(df$LOC, na.rm=TRUE)
code_churn_total   <- sum(df$CODE_CHURN, na.rm=TRUE)
logging_statements <- sum(df$NUM_LOGS, na.rm=TRUE)
log_churn          <- sum((df$LOGADD + df$LOGDEL) * df$TPC, na.rm=TRUE)
pct_with_logging   <- mean(df$NUM_LOGS > 0, na.rm=TRUE) * 100
pct_post_defects   <- mean(df$post_defect_count > 0, na.rm=TRUE) * 100

# ——————————————————————————————————————————————————————————————
# 6) average post‑release defect density
avg_post_with_log    <- mean(df$post_defect_density[df$NUM_LOGS > 0],  na.rm=TRUE)
avg_post_without_log <- mean(df$post_defect_density[df$NUM_LOGS == 0], na.rm=TRUE)
sig_post <- wilcox.test(post_defect_density ~ I(NUM_LOGS > 0), data=df)$p.value < 0.05

# ——————————————————————————————————————————————————————————————
# 7) Spearman vs post‑release defects
spearman_post <- list(
  LOGD               = cor(df$LOGD,             df$post_defect_count, method="spearman", use="complete.obs"),
  LEVELD             = cor(df$LEVELD,           df$post_defect_count, method="spearman", use="complete.obs"),
  LOGADD             = cor(df$LOGADD,           df$post_defect_count, method="spearman", use="complete.obs"),
  LOGDEL             = cor(df$LOGDEL,           df$post_defect_count, method="spearman", use="complete.obs"),
  FCOC               = cor(df$FCOC,             df$post_defect_count, method="spearman", use="complete.obs"),
  PRE                = cor(df$pre_defect_count, df$post_defect_count, method="spearman", use="complete.obs"),
  AVG_LOG_LEN        = cor(df$AVG_LOG_LEN,       df$post_defect_count, method="spearman", use="complete.obs"),
  LOG_SPREAD         = cor(df$LOG_SPREAD,        df$post_defect_count, method="spearman", use="complete.obs"),
  LOG_CODE_OWNERSHIP = cor(df$LOG_CODE_OWNERSHIP,df$post_defect_count, method="spearman", use="complete.obs")
)

# ——————————————————————————————————————————————————————————————
# 8) Spearman among product/process metrics
spearman_pp <- list(
  LOGD_LEVELD         = cor(df$LOGD,      df$LEVELD,     method="spearman", use="complete.obs"),
  LOGADD_FCOC         = cor(df$LOGADD,    df$FCOC,       method="spearman", use="complete.obs"),
  LOGDEL_FCOC         = cor(df$LOGDEL,    df$FCOC,       method="spearman", use="complete.obs"),
  LOGADD_LOGDEL       = cor(df$LOGADD,    df$LOGDEL,     method="spearman", use="complete.obs"),
  AVGLOG_LEN__LOGD    = cor(df$AVG_LOG_LEN, df$LOGD,      method="spearman", use="complete.obs"),
  AVGLOG_LEN__SPREAD  = cor(df$AVG_LOG_LEN, df$LOG_SPREAD,method="spearman", use="complete.obs"),
  SPREAD__LOGD        = cor(df$LOG_SPREAD,  df$LOGD,      method="spearman", use="complete.obs"),
  SPREAD__LEVELD      = cor(df$LOG_SPREAD,  df$LEVELD,    method="spearman", use="complete.obs"),
  AVGLOG_LEN__LEVELD  = cor(df$AVG_LOG_LEN, df$LEVELD,    method="spearman", use="complete.obs")
)

# ——————————————————————————————————————————————————————————————
# 9) PCA‑derived metrics
p_prod <- prcomp(
  scale(df[,c("LOGD","LEVELD","AVG_LOG_LEN","LOG_SPREAD")]),
  center=TRUE, scale.=TRUE
)
df$PRODUCT <- p_prod$x[,1]

p_proc <- prcomp(
  scale(df[,c("LOGADD","FCOC","LOG_CODE_OWNERSHIP")]),
  center=TRUE, scale.=TRUE
)
df$PROCESS <- p_proc$x[,1]

p_tpcpre <- prcomp(
  scale(df[,c("TPC","pre_defect_count")]),
  center=TRUE, scale.=TRUE
)
df$TPCPRE <- p_tpcpre$x[,1]

# ——————————————————————————————————————————————————————————————
# 10) ORIGINAL baseline PCA
p_orig_prod <- prcomp(
  scale(df[,c("LOGD","LEVELD")]),
  center=TRUE, scale.=TRUE
)
df$ORIGINAL_PRODUCT <- p_orig_prod$x[,1]

p_orig_proc <- prcomp(
  scale(df[,c("LOGADD","FCOC")]),
  center=TRUE, scale.=TRUE
)
df$ORIGINAL_PROCESS <- p_orig_proc$x[,1]

# ——————————————————————————————————————————————————————————————
# 11) helpers for logistic models
dev_explained <- function(m) 100*(m$null.deviance - m$deviance) / m$null.deviance
imp_pct       <- function(b,n) round(100 * (n - b) / b)
star          <- function(p) {
  if (p < 0.001) "***"
  else if (p < 0.01) "**"
  else if (p < 0.05) "*"
  else if (p < 0.1) "\u25CA"
  else ""
}
df$has_post <- as.integer(df$post_defect_count > 0)

# ——————————————————————————————————————————————————————————————
# 12) LOC vs LOC+PRODUCT vs LOC+ORIGINAL_PRODUCT
m_loc          <- suppressWarnings(glm(has_post ~ LOC,                    data=df, family="binomial"))
m_loc_prod     <- suppressWarnings(glm(has_post ~ LOC + PRODUCT,          data=df, family="binomial"))
m_loc_orig     <- suppressWarnings(glm(has_post ~ LOC + ORIGINAL_PRODUCT, data=df, family="binomial"))

dev_loc        <- dev_explained(m_loc)
dev_loc_prod   <- dev_explained(m_loc_prod)
dev_loc_orig   <- dev_explained(m_loc_orig)

imp_loc_prod   <- imp_pct(dev_loc, dev_loc_prod)
imp_loc_orig   <- imp_pct(dev_loc, dev_loc_orig)

star_loc_prod  <- star(coef(summary(m_loc_prod))["PRODUCT",           "Pr(>|z|)"])
star_loc_orig  <- star(coef(summary(m_loc_orig))["ORIGINAL_PRODUCT","Pr(>|z|)"])

# ——————————————————————————————————————————————————————————————
# 13) TPCPRE vs TPCPRE+PROCESS vs TPCPRE+ORIGINAL_PROCESS
m_tpc          <- suppressWarnings(glm(has_post ~ TPCPRE,                        data=df, family="binomial"))
m_tpc_proc     <- suppressWarnings(glm(has_post ~ TPCPRE + PROCESS,              data=df, family="binomial"))
m_tpc_orig     <- suppressWarnings(glm(has_post ~ TPCPRE + ORIGINAL_PROCESS,     data=df, family="binomial"))

dev_tpc        <- dev_explained(m_tpc)
dev_tpc_proc   <- dev_explained(m_tpc_proc)
dev_tpc_orig   <- dev_explained(m_tpc_orig)

imp_tpc_proc   <- imp_pct(dev_tpc, dev_tpc_proc)
imp_tpc_orig   <- imp_pct(dev_tpc, dev_tpc_orig)

star_tpc_proc  <- star(coef(summary(m_tpc_proc))["PROCESS",           "Pr(>|z|)"])
star_tpc_orig  <- star(coef(summary(m_tpc_orig))["ORIGINAL_PROCESS","Pr(>|z|)"])

# ——————————————————————————————————————————————————————————————
# 14) LOC+TPCPRE combinations
m_b3        <- suppressWarnings(glm(has_post ~ LOC + TPCPRE,                                 data=df, family="binomial"))
m_b3_p      <- suppressWarnings(glm(has_post ~ LOC + TPCPRE + PRODUCT,                       data=df, family="binomial"))
m_b3_c      <- suppressWarnings(glm(has_post ~ LOC + TPCPRE + PROCESS,                       data=df, family="binomial"))
m_b3_pc     <- suppressWarnings(glm(has_post ~ LOC + TPCPRE + PRODUCT + PROCESS,             data=df, family="binomial"))
m_b3_op     <- suppressWarnings(glm(has_post ~ LOC + TPCPRE + ORIGINAL_PRODUCT,              data=df, family="binomial"))
m_b3_oq     <- suppressWarnings(glm(has_post ~ LOC + TPCPRE + ORIGINAL_PROCESS,              data=df, family="binomial"))
m_b3_oall   <- suppressWarnings(glm(has_post ~ LOC + TPCPRE + ORIGINAL_PRODUCT + ORIGINAL_PROCESS,
                                     data=df, family="binomial"))

dev_b3      <- dev_explained(m_b3)
dev_b3_p    <- dev_explained(m_b3_p)
dev_b3_c    <- dev_explained(m_b3_c)
dev_b3_pc   <- dev_explained(m_b3_pc)
dev_b3_op   <- dev_explained(m_b3_op)
dev_b3_oq   <- dev_explained(m_b3_oq)
dev_b3_oall <- dev_explained(m_b3_oall)

imp_b3_p    <- imp_pct(dev_b3,  dev_b3_p)
imp_b3_c    <- imp_pct(dev_b3,  dev_b3_c)
imp_b3_pc   <- imp_pct(dev_b3,  dev_b3_pc)
imp_b3_op   <- imp_pct(dev_b3,  dev_b3_op)
imp_b3_oq   <- imp_pct(dev_b3,  dev_b3_oq)
imp_b3_oall <- imp_pct(dev_b3,  dev_b3_oall)

star_b3_p     <- star(coef(summary(m_b3_p))["PRODUCT",           "Pr(>|z|)"])
star_b3_c     <- star(coef(summary(m_b3_c))["PROCESS",           "Pr(>|z|)"])
p_fp          <- coef(summary(m_b3_pc))["PRODUCT",           "Pr(>|z|)"]
p_fq          <- coef(summary(m_b3_pc))["PROCESS",           "Pr(>|z|)"]
star_b3_pc    <- if (any(c(p_fp, p_fq) < 0.05)) "**" else ""

star_b3_op   <- star(coef(summary(m_b3_op))["ORIGINAL_PRODUCT",  "Pr(>|z|)"])
star_b3_oq   <- star(coef(summary(m_b3_oq))["ORIGINAL_PROCESS",  "Pr(>|z|)"])
p_op_all     <- coef(summary(m_b3_oall))["ORIGINAL_PRODUCT",  "Pr(>|z|)"]
p_oq_all     <- coef(summary(m_b3_oall))["ORIGINAL_PROCESS",  "Pr(>|z|)"]
star_b3_oall <- if (any(c(p_op_all, p_oq_all) < 0.05)) "**" else ""

# ——————————————————————————————————————————————————————————————
# 15) Effects on the "both original" model for a +10% bump
base_means_o   <- as.list(colMeans(df[,c("LOC","TPCPRE","ORIGINAL_PRODUCT","ORIGINAL_PROCESS")]))
pred0_o        <- predict(m_b3_oall, newdata=base_means_o, type="response")

bm_op          <- base_means_o; bm_op$ORIGINAL_PRODUCT <- bm_op$ORIGINAL_PRODUCT * 1.10
pred1_o        <- predict(m_b3_oall, newdata=bm_op,     type="response")
effect_op      <- 100 * (pred1_o - pred0_o) / pred0_o
star_effect_op <- star(p_op_all)

bm_oq          <- base_means_o; bm_oq$ORIGINAL_PROCESS <- bm_oq$ORIGINAL_PROCESS * 1.10
pred2_o        <- predict(m_b3_oall, newdata=bm_oq,     type="response")
effect_oq      <- 100 * (pred2_o - pred0_o) / pred0_o
star_effect_oq <- star(p_oq_all)

# ——————————————————————————————————————————————————————————————
# 16) assemble & write JSON
out <- list(
  basic = list(
    Lines_of_code            = lines_of_code,
    Code_churn_total         = code_churn_total,
    Logging_statements       = logging_statements,
    Log_churn                = log_churn,
    Pct_files_with_logging   = pct_with_logging,
    Pct_post_release_defects = pct_post_defects
  ),

  defect_density = list(
    avg_post_with_log    = avg_post_with_log,
    avg_post_without_log = avg_post_without_log,
    sig_post_density     = sig_post
  ),

  spearman_post = spearman_post,
  spearman_pp   = spearman_pp,

  Table7 = list(
    Base_LOC                    = dev_loc,
    LOC_plus_PRODUCT            = sprintf("%.2f (+%d%%)%s", dev_loc_prod, imp_loc_prod, star_loc_prod),
    LOC_plus_ORIGINAL_PRODUCT   = sprintf("%.2f (+%d%%)%s", dev_loc_orig, imp_loc_orig, star_loc_orig)
  ),

  Table8 = list(
    Base_TPCPRE                    = dev_tpc,
    TPCPRE_plus_PROCESS            = sprintf("%.2f (+%d%%)%s", dev_tpc_proc, imp_tpc_proc, star_tpc_proc),
    TPCPRE_plus_ORIGINAL_PROCESS   = sprintf("%.2f (+%d%%)%s", dev_tpc_orig, imp_tpc_orig, star_tpc_orig)
  ),

  Table9 = list(
    Base_LOC_TPCPRE               = dev_b3,
    plus_PRODUCT                  = sprintf("%.2f (+%d%%)%s", dev_b3_p, imp_b3_p, star_b3_p),
    plus_PROCESS                  = sprintf("%.2f (+%d%%)%s", dev_b3_c, imp_b3_c, star_b3_c),
    plus_both                     = sprintf("%.2f (+%d%%)%s", dev_b3_pc, imp_b3_pc, star_b3_pc),
    plus_ORIGINAL_PRODUCT         = sprintf("%.2f (+%d%%)%s", dev_b3_op, imp_b3_op, star_b3_op),
    plus_ORIGINAL_PROCESS         = sprintf("%.2f (+%d%%)%s", dev_b3_oq, imp_b3_oq, star_b3_oq),
    plus_ORIGINAL_BOTH            = sprintf("%.2f (+%d%%)%s", dev_b3_oall, imp_b3_oall, star_b3_oall)
  ),

  original_effects = list(
    Effect_ORIGINAL_PRODUCT = sprintf("%.1f%%%s", effect_op, star_effect_op),
    Effect_ORIGINAL_PROCESS = sprintf("%.1f%%%s", effect_oq, star_effect_oq)
  )
)

jsonfile <- paste0("process-", sub("\\.csv$","", basename(csvfile)), ".json")
write(toJSON(out, pretty=TRUE, auto_unbox=TRUE), file=jsonfile)
cat("Wrote metrics to", jsonfile, "\n")
