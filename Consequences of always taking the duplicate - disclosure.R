
#"The Census Bureau has reviewed this data product to ensure appropriate access, use, and disclosure avoidance protection of the confidential 
# source data used to produce this product (Data Management System (DMS) number: P-7504847, subproject P-7514952, Disclosure Review Board (DRB)
# approval number: CBDRB-FY22-EWD001-007)."



############################################################################################################
# Anahi Rodriguez
# calculating consequences in dollar amounts for always taking the duplicate vs case-by-case basis
# start with replaced files indicators
# next is non replaced files indicators
# tables for conseuqences created in powerpoint updates for Emily and Anne 
# 7/22/22
############################################################################################################





#set up

library(DBI)
library(dbplyr)
library(tidyr)
library(Hmisc)
library(stringr)
library(gridExtra)
library(writexl)



#sign in

pass = rstudioapi::askForPassword("Database password")
con_string = paste0("some path here", 
                    pass)

con = dbConnect(odbc::odbc(), .connection_string = con_string, timeout = 10)


# -------------------------- sql queries for needed data ------------------------------------------------


for_de_count = build_sql(
  "select  distinct data_element, Table_A.parent_cfn, Table_B.response_batch,
          Table_A.ref_per,Table_A.duplicate_indicator,
          Table_B.item_value_orig, Table_B.dupe_typ,
          Table_C.survunit_typ,Table_C.dup_count,
          Table_D.repeat_group_num, Table_B.child_cfn
  from Table_A  
  left join Table_B
     on Table_A.parent_cfn = Table_B.parent_cfn and
     Table_A.ref_per = Table_B.ref_per 
  left join Table_C
    on Table_B.parent_cfn = Table_C.survunit_id and
    Table_B.ref_per = Table_C.ref_per
  left join Table_D
     on Table_C.survunit_id = Table_D.survunit_id and
     Table_C.ref_per = Table_D.ref_per and
     Table_D.repeat_group_num = Table_B.repeat_group_num
  where
      Table_A.ref_per = '2017U1' and
      Table_B.ref_per = '2017U1' and
      Table_C.ref_per = '2017U1' and
      Table_D.ref_per = '2017U1' and
      Table_A.auto_resolved_stat = 'N' and
      Table_D.repeat_group_num = '0' 
      and dup_count = '1' and data_element in 
      ('RCPT_TOT','PAY_ANN','PAY_QTR1','EMP_MAR12','EMPQ1') and
      Table_A.duplicate_indicator in ('A','R','S','W','Z','G', 'Q', 'U', 'V')",
  con = con)

for_de_count_df = DBI::dbGetQuery(con,for_de_count)


# pull any data elements and prod numbers for original batches submitted for same indicators as above

for_original_de_count = build_sql(
  "select  distinct data_element, Table_A.parent_cfn, Table_B.response_batch,
          Table_A.ref_per,Table_A.duplicate_indicator,
          Table_B.item_value_orig, Table_B.dupe_typ,
          Table_C.survunit_typ,Table_C.dup_count,
          Table_D.repeat_group_num, Table_B.child_cfn,
          Table_B.trade_assignment
  from Table_A  
  left join Table_B
     on Table_A.parent_cfn = Table_B.parent_cfn and
     Table_A.ref_per = Table_B.ref_per 
  left join Table_C
    on Table_B.parent_cfn = Table_C.survunit_id and
    Table_B.ref_per = Table_C.ref_per
  left join Table_D
     on Table_C.survunit_id = Table_D.survunit_id and
     Table_C.ref_per = Table_D.ref_per and
     Table_D.repeat_group_num = Table_B.repeat_group_num
  where
      Table_A.ref_per = '2017U1' and
      Table_B.ref_per = '2017U1' and
      Table_C.ref_per = '2017U1' and
      Table_D.ref_per = '2017U1' and
      Table_A.auto_resolved_stat = 'N' and
      Table_D.repeat_group_num = '0' 
      and dup_count = '1' and data_element in 
      ('RCPT_TOT','PAY_ANN','PAY_QTR1','EMP_MAR12','EMPQ1') and
      Table_B.dupe_typ is null and
      Table_A.duplicate_indicator in ('A','R','S','W','Z', 'G', 'Q', 'U', 'V')",
  con = con)

for_original_de_count_df = DBI::dbGetQuery(con,for_original_de_count)


# pull any data elements and prod numbers for dupe batches submitted for same indicators as above

for_dupe_de_count = build_sql(
  "select  distinct data_element, Table_A.parent_cfn, Table_B.response_batch,
          Table_A.ref_per,Table_A.duplicate_indicator,
          Table_B.item_value_orig, Table_B.dupe_typ,
          Table_C.survunit_typ,Table_C.dup_count,
          Table_D.repeat_group_num, Table_B.child_cfn,
          Table_B.trade_assignment
  from Table_A  
  left join Table_B
     on Table_A.parent_cfn = Table_B.parent_cfn and
     Table_A.ref_per = Table_B.ref_per 
  left join Table_C
    on Table_B.parent_cfn = Table_C.survunit_id and
    Table_B.ref_per = Table_C.ref_per
  left join Table_D
     on Table_C.survunit_id = Table_D.survunit_id and
     Table_C.ref_per = Table_D.ref_per and
     Table_D.repeat_group_num = Table_B.repeat_group_num
  where
      Table_A.ref_per = '2017U1' and
      Table_B.ref_per = '2017U1' and
      Table_C.ref_per = '2017U1' and
      Table_D.ref_per = '2017U1' and
      Table_A.auto_resolved_stat = 'N' and
      Table_D.repeat_group_num = '0' 
      and dup_count = '1' and data_element in 
      ('RCPT_TOT','PAY_ANN','PAY_QTR1','EMP_MAR12','EMPQ1') and
      dupe_typ in ('A','X','M','B','T','C','R','D') and
      Table_A.duplicate_indicator in ('A','R','S','W','Z','G', 'Q', 'U', 'V')",
  con = con)

for_dupe_de_count_df = DBI::dbGetQuery(con,for_dupe_de_count)



#--------------------------  REMOVE B BATCES AND FALSE DUPES  ------------------------------------ 


# remove any parent cfn that has an existing B batch because those bypass dupe review system
# DLQs are not true duplicates
# DATA_TRANSFERS can override original submissions but in every case I found, DATA_TRANSFERS WERE NOT TAKen



check_df = read_excel("2017_PARENT_CFNS_B_BATCHES_AND_PR_BATCHES.xlsx")



for_original_de_count_df = for_original_de_count_df %>%
  filter((!PARENT_CFN %in% check_df$PARENT_CFN ) 
         & (!grepl('MUDLQ|SUDLQ|DATA_TRANSFER|MU_COMB|0292000',RESPONSE_BATCH))
         & (grepl('K|G' ,RESPONSE_BATCH)))



for_dupe_de_count_df = for_dupe_de_count_df %>%
  filter((!PARENT_CFN %in% check_df$PARENT_CFN )
         & (!grepl('MUDLQ|SUDLQ|DATA_TRANSFER|MU_COMB|0292000',RESPONSE_BATCH))
         & (grepl('K|G' ,RESPONSE_BATCH)))





# -------------------------------- calculating aggregate totals for revenue and payroll ---------------------------

# researching into revenue and payroll dollar totals for better idea of consequences of always taking the duplicate compared to the 2017 analysis
for_dupe_de_count_df$ITEM_VALUE_ORIG = as.numeric(for_dupe_de_count_df$ITEM_VALUE_ORIG)
for_original_de_count_df$ITEM_VALUE_ORIG = as.numeric(for_original_de_count_df$ITEM_VALUE_ORIG)

# add replaced/ not replaced column
for_dupe_de_count_df$REPLACED = ifelse(for_dupe_de_count_df$DUPLICATE_INDICATOR %in% c('A','R','S','W','Z'), "YES","NO")

for_original_de_count_df$REPLACED = ifelse(for_original_de_count_df$DUPLICATE_INDICATOR %in% c('A','R','S','W','Z'), "YES","NO")

# trying to fix data slides
colnames(for_dupe_de_count_df) = c("DATA_ELEMENT","PARENT_CFN","DUPLICATE_BATCH",
                                   "REF_PER","DUPLICATE_INDICATOR","DUPE_ITEM_VALUE_ORIG",
                                   "DUPE_TYP","SURVUNIT_TYP","DUP_COUNT","REPEAT_GROUP_NUM",
                                   "CHILD_CFN", "TRADE_ASSIGNMENT","REPLACED")
colnames(for_original_de_count_df) = c("DATA_ELEMENT","PARENT_CFN","ORIGINAL_BATCH",
                                       "REF_PER","DUPLICATE_INDICATOR","OG_ITEM_VALUE_ORIG",
                                       "DUPE_TYP","SURVUNIT_TYP","DUP_COUNT","REPEAT_GROUP_NUM",
                                       "CHILD_CFN", "TRADE_ASSIGNMENT", "REPLACED")

# drop variables for merge
combined = for_original_de_count_df %>%
  select(-c("DUPLICATE_INDICATOR","DUPE_TYP")) %>%
  full_join(for_dupe_de_count_df, 
            by = c('PARENT_CFN','CHILD_CFN','DATA_ELEMENT',
                   'REF_PER','SURVUNIT_TYP','DUP_COUNT',
                   'REPEAT_GROUP_NUM','REPLACED')) %>%
  # reorder these for convenience
  select(c(PARENT_CFN,CHILD_CFN,DATA_ELEMENT,DUPE_TYP,ORIGINAL_BATCH,OG_ITEM_VALUE_ORIG,
           DUPLICATE_BATCH, DUPE_ITEM_VALUE_ORIG,DUPLICATE_INDICATOR, SURVUNIT_TYP,
           TRADE_ASSIGNMENT.x, REPLACED))


# fix data slides
dupe_typ_r = combined %>%
  filter(DUPE_TYP == 'R')

# divide by 1000 where there exists a number 1000ish times larger than the other entered value
dupe_typ_r$OG_ITEM_VALUE_ORIG = ifelse(dupe_typ_r$OG_ITEM_VALUE_ORIG > dupe_typ_r$DUPE_ITEM_VALUE_ORIG
                                       & ((dupe_typ_r$OG_ITEM_VALUE_ORIG - dupe_typ_r$DUPE_ITEM_VALUE_ORIG) > 10 ),
                                       dupe_typ_r$OG_ITEM_VALUE_ORIG/1000, dupe_typ_r$OG_ITEM_VALUE_ORIG)

# rounding after dividing by 1000
dupe_typ_r$OG_ITEM_VALUE_ORIG = round(dupe_typ_r$OG_ITEM_VALUE_ORIG, 0)


dupe_typ_r$DUPE_ITEM_VALUE_ORIG = ifelse(dupe_typ_r$DUPE_ITEM_VALUE_ORIG > dupe_typ_r$OG_ITEM_VALUE_ORIG
                                         & ((dupe_typ_r$DUPE_ITEM_VALUE_ORIG - dupe_typ_r$OG_ITEM_VALUE_ORIG) > 10 ),
                                         dupe_typ_r$DUPE_ITEM_VALUE_ORIG/1000, dupe_typ_r$DUPE_ITEM_VALUE_ORIG)
# rounding after dividing by 1000
dupe_typ_r$DUPE_ITEM_VALUE_ORIG = round(dupe_typ_r$DUPE_ITEM_VALUE_ORIG, 0)


# remove old observations for dupe type R and replace with new observations
combined = combined %>%
  filter(DUPE_TYP != 'R')

combined = rbind(combined,dupe_typ_r)

# summing up the entered values for both rcpt_tot and pay_ann

# revenue in terms of dollar amts
revenue_totals = combined %>%
  filter(DATA_ELEMENT == 'RCPT_TOT')
# revenue for replaced cases
revenue_replaced = revenue_totals %>%
  filter(REPLACED == 'YES')
# total revenue for original batches among files that were replaced
signif(sum(revenue_replaced$OG_ITEM_VALUE_ORIG, na.rm = T),4) 
# total revenue for duplicate batches among files that were replaced
signif(sum(revenue_replaced$DUPE_ITEM_VALUE_ORIG, na.rm = T), 4) 

# revenue for not replaced cases
revenue_not_replaced = revenue_totals %>%
  filter(REPLACED == 'NO')
# total revenue for original batches among files that were not replaced
signif(sum(revenue_not_replaced$OG_ITEM_VALUE_ORIG, na.rm = T), 4)
signif(sum(revenue_not_replaced$DUPE_ITEM_VALUE_ORIG, na.rm =T), 4) 



# payrol in terms of dollar amts
payroll_totals = combined %>%
  filter(DATA_ELEMENT == 'PAY_ANN')
# revenue for replaced cases
payroll_replaced = payroll_totals %>%
  filter(REPLACED == 'YES')
# total revenue for original batches among files that were replaced
signif(sum(payroll_replaced$OG_ITEM_VALUE_ORIG, na.rm = T),4) 
# total revenue for duplicate batches among files that were replaced
signif(sum(payroll_replaced$DUPE_ITEM_VALUE_ORIG, na.rm = T),4) 

# payroll for not replaced cases
payroll_not_replaced = payroll_totals %>%
  filter(REPLACED == 'NO')
# total revenue for original batches among files that were not replaced
signif(sum(payroll_not_replaced$OG_ITEM_VALUE_ORIG, na.rm = T),4) 
signif(sum(payroll_not_replaced$DUPE_ITEM_VALUE_ORIG, na.rm =T),4) 



DBI::dbDisconnect(con)


