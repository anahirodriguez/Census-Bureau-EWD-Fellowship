
#"The Census Bureau has reviewed this data product to ensure appropriate access, use, and disclosure avoidance protection of the confidential 
# source data used to produce this product (Data Management System (DMS) number: P-7504847, subproject P-7514952, Disclosure Review Board (DRB)
# approval number: CBDRB-FY22-EWD001-007)."




############################################################################################################
# Anahi Rodriguez
# SQl query to identify response batch PROD_#### count
# added count of core data_elements as a own column
# creating table for future use - one with product number counts as well as core variable counts 
# use that table ^ to create another table in future but likely do not need again because important values are also stored in other table
# other table mentioned above is created in "Creating Sample for Logisitc Regression" script also in duplciate-review folder
# start with replaced files indicators
# next is non replaced files indicators
# 7/15/22
############################################################################################################



#set up

library(DBI)
library(dbplyr)
library(dplyr)
library(tidyr)
library(Hmisc)
library(stringr)
library(gridExtra)
library(writexl)



#sign in

pass = rstudioapi::askForPassword("Database password")
con_string = paste0("some database path here", 
                    pass)

con = dbConnect(odbc::odbc(), .connection_string = con_string, timeout = 10)




#----------------------------------------- prod number counts for original and dupe batch -----------------------------------------------------


# pulling all prod_# 
# will work on pulling core data elements later -- need to filter out where prod_# = 0, but 0 could be useful info for core variables
# so need to do them seperately



for_prod_count = build_sql(
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
      and dup_count = '1' and (data_element like 'PROD_%' or data_element like 'NAICS_%') 
      and Table_A.duplicate_indicator in ('A','R','S','W','Z',
                                                          'G','V','U','Q')",
  con = con)

for_prod_count_df = DBI::dbGetQuery(con,for_prod_count)



#--------------------------  REMOVE B BATCES AND FALSE DUPES  ------------------------------------

# remove any parent cfn that has an existing B batch because those bypass dupe review system
# DLQs are not true duplicates
# DATA_TRANSFERS can override original submissions but in every case I found, DATA_TRANSFERS WERE NOT TAKen
# similarly, MUCOMB onyl has some cases and in all, they were not taken as final

check_df = read_excel("2017_PARENT_CFNS_B_BATCHES_AND_PR_BATCHES.xlsx")


for_de_count_df = for_de_count_df %>%
  filter(!PARENT_CFN %in% check_df$PARENT_CFN)



for_prod_count_df = for_prod_count_df %>%
  filter(!PARENT_CFN %in% check_df$PARENT_CFN)


for_prod_count_df = for_prod_count_df%>%
  filter(grepl('K|G' ,RESPONSE_BATCH))

# prod/naics numbers input with 0 because no info given there 

for_prod_count_df = for_prod_count_df %>%
  filter(ITEM_VALUE_ORIG != "0.00" & ITEM_VALUE_ORIG != "00" & ITEM_VALUE_ORIG != "000" & 
           ITEM_VALUE_ORIG != "00000" & ITEM_VALUE_ORIG != "0000000000000" & ITEM_VALUE_ORIG != "0" 
         &  ITEM_VALUE_ORIG != "-0" & ITEM_VALUE_ORIG != "N/A" & ITEM_VALUE_ORIG != "$0.00"
         & ITEM_VALUE_ORIG != "$0" & ITEM_VALUE_ORIG != "#VALUE!")


# seperate into replaced/not replaced indicators and original vs duplicates to do counts

for_prod_count_df1 = for_prod_count_df %>%
  filter(DUPLICATE_INDICATOR %in% c('G','Q','U','V'))

for_prod_count_df = for_prod_count_df %>%
  filter(DUPLICATE_INDICATOR %in% c('A','R','Z','W','S'))

for_prod_count_original_replaced_df = for_prod_count_df %>%
  filter(is.na(DUPE_TYP))

for_prod_count_dupe_replaced_df = for_prod_count_df %>%
  filter(!is.na(DUPE_TYP))

for_prod_count_original_not_replaced_df = for_prod_count_df1 %>%
  filter(is.na(DUPE_TYP))

for_prod_count_dupe_not_replaced_df = for_prod_count_df1 %>%
  filter(!is.na(DUPE_TYP))


# -------------------- counts for product numbers for replaced indicators----------------------------------------------

# counts to use later -- number of core variables questions we are interested in that occur per batch
prod_count = for_prod_count_df %>%
  group_by(PARENT_CFN) %>%
  count(RESPONSE_BATCH)


# all unique parent_cfns for cases where at least one product appeared between original and dupe batch
newtable = data.frame(PARENT_CFN = unique(for_prod_count_df$PARENT_CFN))

# start with placing duplicate batch numbers next to parent_cfn
formerge_dupe = for_prod_count_dupe_replaced_df%>%
  select(PARENT_CFN,RESPONSE_BATCH, TRADE_ASSIGNMENT)

formerge_dupe = unique(formerge_dupe)


first = full_join(formerge_dupe, newtable, by = "PARENT_CFN")

# next place original batch numbers next to parent_cfn and duplicate batch number -- need to rename RESPONSE_BATCH to merge
formerge_original = for_prod_count_original_replaced_df%>%
  select(PARENT_CFN,RESPONSE_BATCH)

formerge_original = unique(formerge_original)


colnames(formerge_original) <- c("PARENT_CFN", "ORIGINAL_BATCH")
colnames(first) <- c("PARENT_CFN","DUPLICATE_BATCH", "TRADE_ASSIGNMENT")

second = full_join(first,formerge_original, by = "PARENT_CFN")


# next, need to be able to fill in batch numbers where either dupe or original had some product numbers and other had 0 product numbers
# us NAs to fill prod_count with 0 first - this is where one of the two batches had no prod numbers but the other had at least one
# because one batch contained no prod numbers, its response_batch wasn't pulled and needs to be added now instead
second[is.na(second)] <- 0

second$original_prod_count = ""
second$original_prod_count = ifelse(second$ORIGINAL_BATCH == 0, 0,NA)

second$dupe_prod_count = ""
second$dupe_prod_count = ifelse(second$DUPLICATE_BATCH == 0, 0 , NA)


# pulling missing batch numbers for when original batch had no prod numbers but dupe did

missing_rb = second %>%
  filter(ORIGINAL_BATCH == '0')%>%
  select(PARENT_CFN)


for_missing_rb = build_sql(
  "select distinct response_batch, parent_cfn
  from Table_B
  where ref_per = '2017U1' and
  (response_batch like 'G%' or
  response_batch like 'K%') and
  dupe_typ is null",
  con = con
  
)

for_missing_rb_df = DBI::dbGetQuery(con,for_missing_rb)

for_missing_rb_df = for_missing_rb_df %>%
  filter(!PARENT_CFN %in% check_df$PARENT_CFN)


# repeating steps above but this time for when the duplicate batch had no prod numbers but the original did
missing_rb_dupe = second %>%
  filter(DUPLICATE_BATCH == '0')%>%
  select(PARENT_CFN)


for_missing_rb_dupe = build_sql(
  "select distinct response_batch, parent_cfn
  from Table_A
  where ref_per = '2017U1' and
  (response_batch like 'G%' or
  response_batch like 'K%')",
  con = con
  
)

for_missing_rb_dupe_df = DBI::dbGetQuery(con,for_missing_rb_dupe)

# removing b batches from this - probably not necessary if I do a left_join after with this as the second table
for_missing_rb_dupe_df = for_missing_rb_dupe_df %>%
  filter(!PARENT_CFN %in% check_df$PARENT_CFN)



# to be able to merge - replace filled in 0s with NAs again  (only used 0s to be able to pull missing response batch numbers)
# change back to NAs for merge
second$ORIGINAL_BATCH = ifelse(second$ORIGINAL_BATCH == 0, NA, second$ORIGINAL_BATCH)


third = left_join(x = missing_rb, y = for_missing_rb_df, by = "PARENT_CFN")

# cases where no original batch exists in the BR - total of 288 cases
# dont need no_original_batch but nice to have to look at specific weird cases ^^
# filter them out of third after
no_original_batch = third %>%
  filter(is.na(RESPONSE_BATCH))


third = third %>%
  filter(!is.na(RESPONSE_BATCH))

second = second %>%
  filter(!PARENT_CFN %in% no_original_batch$PARENT_CFN)

colnames(third) <- c("PARENT_CFN","ORIGINAL_BATCH")

# merge to fill in missing original batch numbers - dont merge on ORIGINAL_BATCH because not all will have original batch in second
# need to remove cases with no original batch number from 'second' df also
fourth = full_join(second, third, by = c("PARENT_CFN"))

# use new column to fill in missing response batch values
fourth$ORIGINAL_BATCH.x = ifelse(is.na(fourth$ORIGINAL_BATCH.x), fourth$ORIGINAL_BATCH.y, fourth$ORIGINAL_BATCH.x)

# drop new column - dont need anymore
fourth = fourth %>%
  select(-c(ORIGINAL_BATCH.y))

# instead of repeating above steps for missing duplicate batch numbers, drop 4 rows with missing info because I wasnt able to pull trade area
# skip fourth to fifth to match rest of code following this 
sixth = fourth %>%
  filter(!TRADE_ASSIGNMENT == '0')

# reorder columns 
sixth = sixth %>%
  select(c(PARENT_CFN,DUPLICATE_BATCH, ORIGINAL_BATCH.x, original_prod_count, dupe_prod_count, TRADE_ASSIGNMENT))

## adding in actual prod/naics counts - still working with replaced only

# adding counts to each batch per parent
#TEMPORARILY CALL DUPLICATE_BATCH RESPONSE_BATCH

colnames(sixth) <- c("PARENT_CFN", "RESPONSE_BATCH","ORIGINAL_BATCH","original_prod_count","dupe_prod_count", "TRADE")

# adding column with count of total product numbers or naics codes within each batch 
# start with adding a column with the count and then move the count into appropriate column
seven = left_join(sixth,prod_count, by = c("PARENT_CFN","RESPONSE_BATCH"))
# move numbers over to correct column and rename columns
seven$dupe_prod_count = ifelse(is.na(seven$n), 0, seven$n )
seven = seven %>%
  select(-n)

# continuing adding counts but for original now
#TEMPORARILY CALL ORIGINAL BATCH RESPONSE_BATCH
colnames(seven) <- c("PARENT_CFN", "DUPLICATE_BATCH","RESPONSE_BATCH","OG_PROD_COUNT","DUPE_PROD_COUNT", "TRADE")


# move numbers over to correct column and rename columns
eight = left_join(seven, prod_count, by = c("PARENT_CFN","RESPONSE_BATCH"))
eight$OG_PROD_COUNT = ifelse(is.na(eight$n), 0 , eight$n)
eight = eight%>%
  select(-n)
colnames(eight)<- c("PARENT_CFN","DUPLICATE_BATCH","ORIGINAL_BATCH","OG_PROD_COUNT", "DUPE_PROD_COUNT", "TRADE")


# COMPARE COUNTS
eight$dupe_more_or_same = ifelse(eight$OG_PROD_COUNT<=eight$DUPE_PROD_COUNT, "yes","no")


# -------------------- counts for product numbers for not replaced indicators----------------------------------------------

# counts to use later -- number of PROD_# that occur per batch
prod_count1 = for_prod_count_df1 %>%
  group_by(PARENT_CFN) %>%
  count(RESPONSE_BATCH)

# all unique parent_cfns for cases where at least one prod_# appeared between orignal and dupe
newtable1 = data.frame(PARENT_CFN = unique(for_prod_count_df1$PARENT_CFN))

# start with placing duplicate batch numbers next to parent_cfn
formerge_dupe1 = for_prod_count_dupe_not_replaced_df%>%
  select(PARENT_CFN,RESPONSE_BATCH, TRADE_ASSIGNMENT)

formerge_dupe1 = unique(formerge_dupe1)


first1 = full_join(formerge_dupe1, newtable1, by = "PARENT_CFN")

# next place orignal batch numbers next to parent_cfn and duplicate batch number -- need to rename RESPONSE_BATCH to merge
formerge_original1 = for_prod_count_original_not_replaced_df%>%
  select(PARENT_CFN,RESPONSE_BATCH)

formerge_original1 = unique(formerge_original1)


colnames(formerge_original1) <- c("PARENT_CFN", "ORIGINAL_BATCH")
colnames(first1) <- c("PARENT_CFN","DUPLICATE_BATCH", "TRADE_ASSIGNMENT")

second1 = full_join(first1,formerge_original1, by = "PARENT_CFN")

# next, need to be able to fill in batch numbers where either dupe or original had some product numbers and other had 0 product numbers
# use NAs to fill prod_count with 0 first - this is where one of the two batches had no prod numbers but the other had at least one
# because one batch contained no prod numbers, its response_batch wasn't pulled and needs to be added now instead
second1[is.na(second1)] <- 0

second1$original_prod_count = ""
second1$original_prod_count = ifelse(second1$ORIGINAL_BATCH == 0, 0,NA)

second1$dupe_prod_count = ""
second1$dupe_prod_count = ifelse(second1$DUPLICATE_BATCH == 0, 0 , NA)


# need orignial response batch numbers for original batches that contained no prod numbers at all
missing_rb1 = second1 %>%
  filter(ORIGINAL_BATCH == '0')%>%
  select(PARENT_CFN)


missing_rb_dupe1 = second1 %>%
  filter(DUPLICATE_BATCH == '0')%>%
  select(PARENT_CFN)


# to be able to merge - replace filled in 0s with NAs again  (only used 0s to be able to pull missing response batch numbers)
# change back to NAs for merge
second1$ORIGINAL_BATCH = ifelse(second1$ORIGINAL_BATCH == 0, NA, second1$ORIGINAL_BATCH)


third1 = left_join(x = missing_rb1, y = for_missing_rb_df, by = "PARENT_CFN")

# cases where no original batch exists in the BR 
# dont need no_original_batch but nice to have to look at specific weird cases ^^
# filter them out of third after

no_original_batch1 = third1 %>%
  filter(is.na(RESPONSE_BATCH))

third1 = third1 %>%
  filter(!is.na(RESPONSE_BATCH))


second1 = second1 %>%
  filter(!PARENT_CFN %in% no_original_batch1$PARENT_CFN)


colnames(third1) <- c("PARENT_CFN","ORIGINAL_BATCH")


#merge to fill in missing original batch numbers - dont merge on ORIGINAL_BATCH because not all will have original batch in second
# need to remove cases with no original batch number from 'second' df also
fourth1 = full_join(second1, third1, by = c("PARENT_CFN"))


# use new column to fill in missing response batch values
fourth1$ORIGINAL_BATCH.x = ifelse(is.na(fourth1$ORIGINAL_BATCH.x), fourth1$ORIGINAL_BATCH.y, fourth1$ORIGINAL_BATCH.x)

fourth1 = fourth1 %>%
  select(-c(ORIGINAL_BATCH.y))


# need duplicate response batch numbers for original batches that contained no prod numbers at all
# this is some cases, but i will drop those rows becuase trade area will not be pulled

sixth1 = fourth1 %>%
  filter(!TRADE_ASSIGNMENT == '0')

# reorder columns
sixth1 = sixth1 %>%
  select(c(PARENT_CFN, DUPLICATE_BATCH, ORIGINAL_BATCH.x, original_prod_count, dupe_prod_count,TRADE_ASSIGNMENT))


# -------------------------------------------- adding in actual counts --------------------------------

#TEMPORARILY CALL DUPLICATE_BATCH RESPONSE_BATCH
colnames(sixth1) <- c("PARENT_CFN", "RESPONSE_BATCH","ORIGINAL_BATCH",
                      "original_prod_count","dupe_prod_count", "TRADE'")

# adding column with count of total product numbers or naics codes within each batch 
# start with adding a column with the count and then move the count into appropriate column
seven1 = left_join(sixth1,prod_count1, by = c("PARENT_CFN","RESPONSE_BATCH"))

# move numbers over to correct column and rename columns
seven1$dupe_prod_count = ifelse(is.na(seven1$n), 0, seven1$n )

seven1 = seven1 %>%
  select(-n)

# continuing adding counts but for original now

# TEMPORARILY CALL ORIGINAL BATCH RESPONSE_ATCH
colnames(seven1) <- c("PARENT_CFN", "DUPLICATE_BATCH","RESPONSE_BATCH",
                      "OG_PROD_COUNT","DUPE_PROD_COUNT", "TRADE")

# continuing adding counts but for original now
#TEMPORARILY CALL ORIGINAL BATCH RESPONSE_BATCH
eight1 = left_join(seven1, prod_count1, by = c("PARENT_CFN","RESPONSE_BATCH"))

# move numbers over ot correct column and rename columns
eight1$OG_PROD_COUNT = ifelse(is.na(eight1$n), 0 , eight1$n)

eight1 = eight1%>%
  select(-n)

# final naming of columns
colnames(eight1)<- c("PARENT_CFN","DUPLICATE_BATCH","ORIGINAL_BATCH",
                     "OG_PROD_COUNT","DUPE_PROD_COUNT", "TRADE")


# COMPARE COUNTS
eight1$dupe_more_or_same = ifelse(eight1$OG_PROD_COUNT<=eight1$DUPE_PROD_COUNT, "yes","no")


################################## repeat above processes but for core data_elements now ##################################################

# will again seperate into reviewed and replaced & reviewed and not replaced
# this time for core variables: PAY_ANN, PAY_QTR1, EMP_MAR12, RCPT_TOT, & COST_MAT_TOT
# here, do not factor out 0s becuase they can be telling information here
# would eventaully like to add more data elements specific to manufacturing

# begining of core data element counts for replaced files: A,R,S,W,Z

for_de_count = build_sql(
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
      ('RCPT_TOT','PAY_ANN','PAY_QTR1','EMP_MAR12','EMPQ1', 'COST_MAT_TOT') and
      Table_A.duplicate_indicator in ('A','R','S','W','Z', 'G','U','Q','V')",
  con = con)

for_de_count_df = DBI::dbGetQuery(con,for_de_count)



#--------------------------  REMOVE B BATCES AND FALSE DUPES  ------------------------------------ 

# remove any parent cfn that has an existing B batch because those bypass dupe review system
# DLQs are not true duplicates
# DATA_TRANSFERS can override original submissions but in every case I found, DATA_TRANSFERS WERE NOT TAKen
# similarly, MUCOMB onyl has some cases and in all, they were not taken as final

for_de_count_df = for_de_count_df %>%
  filter(!PARENT_CFN %in% check_df$PARENT_CFN)


for_de_count_df = for_de_count_df%>%
  filter(grepl('K|G' ,RESPONSE_BATCH))



# seperate into replaced/not replaced indicators and original vs duplicates to do counts

for_de_count_df1 = for_de_count_df %>%
  filter(DUPLICATE_INDICATOR %in% c('G','Q','U','V'))

for_de_count_df = for_de_count_df %>%
  filter(DUPLICATE_INDICATOR %in% c('A','R','Z','W','S'))


for_original_de_count_df = for_de_count_df %>%
  filter(is.na(DUPE_TYP))

for_dupe_de_count_df = for_de_count_df %>%
  filter(!is.na(DUPE_TYP))

for_original_de_count1_df = for_de_count_df1 %>%
  filter(is.na(DUPE_TYP))

for_dupe_de_count1_df = for_de_count_df1 %>%
  filter(!is.na(DUPE_TYP))



# -------------------- counts for core data elements for replaced indicators----------------------------------------------

# counts to use later -- number of core variables we are interested in that occur per batch
de_count = for_de_count_df %>%
  group_by(PARENT_CFN) %>%
  count(RESPONSE_BATCH)


# all unique parent_cfns for cases where at least one core variable question appeared between orignal and dupe
newtable3 = data.frame(PARENT_CFN = unique(for_de_count_df$PARENT_CFN))

# start with placing duplicate batch numbers next to parent_cfn
formerge_dupe3 = for_dupe_de_count_df%>%
  select(PARENT_CFN,RESPONSE_BATCH, TRADE_ASSIGNMENT)

formerge_dupe3 = unique(formerge_dupe3)


first3 = full_join(formerge_dupe3, newtable3, by = "PARENT_CFN")

# next place orignal batch numbers next to parent_cfn and duplicate batch number -- need to rename RESPONSE_BATCH to merge
formerge_original3 = for_original_de_count_df%>%
  select(PARENT_CFN,RESPONSE_BATCH)

formerge_original3 = unique(formerge_original3)


colnames(formerge_original3) <- c("PARENT_CFN", "ORIGINAL_BATCH")
colnames(first3) <- c("PARENT_CFN","DUPLICATE_BATCH", "TRADE_ASSIGNMENT")

second3 = full_join(first3,formerge_original3, by = "PARENT_CFN")


# next, need to be able to fill in batch numbers where either dupe or original had some and other had 0 data elements
# us NAs to fill prod_count with 0 first - this is where one of the two batches had no core variable questions answered but the other had at least one
# because one batch contained no core variable answers, its response_batch wasn't pulled and needs to be added now instead

second3[is.na(second3)] <- 0

second3$original_prod_count = ""
second3$original_prod_count = ifelse(second3$ORIGINAL_BATCH == 0, 0,NA)

second3$dupe_prod_count = ""
second3$dupe_prod_count = ifelse(second3$DUPLICATE_BATCH == 0, 0 , NA)


# pulling missing batch numbers for when original batch had no core data elements but dupe did
missing_rb3 = second3 %>%
  filter(ORIGINAL_BATCH == '0')%>%
  select(PARENT_CFN)


# repeating steps above but this time for when the duplicate batch had no core data elements but the original did
missing_rb_dupe3 = second3 %>%
  filter(DUPLICATE_BATCH == '0')%>%
  select(PARENT_CFN)

# to be able to merge - replace filled in 0s with NAs again  (onyl used 0s to be able to pull missing response batch numbers)
# change back to NAs for merge
second3$ORIGINAL_BATCH = ifelse(second3$ORIGINAL_BATCH == 0, NA, second3$ORIGINAL_BATCH)


third3 = left_join(x = missing_rb3, y = for_missing_rb_df, by = "PARENT_CFN")

# cases where no original batch exists in the BR 
# dont need no_original_batch but nice to have to look at specific weird cases ^^
# filter them out of third after
no_original_batch3 = third3 %>%
  filter(is.na(RESPONSE_BATCH))


third3 = third3 %>%
  filter(!is.na(RESPONSE_BATCH))


second3 = second3 %>%
  filter(!PARENT_CFN %in% no_original_batch3$PARENT_CFN)


colnames(third3) <- c("PARENT_CFN","ORIGINAL_BATCH")

# merge to fill in missing original batch numbers - dont merge on ORIGINAL_BATCH because not all will have original batch in second
# need to remove cases with no original batch number from 'second' df also
fourth3 = full_join(second3, third3, by = c("PARENT_CFN"))

# use new column to fill in missing response batch values
fourth3$ORIGINAL_BATCH.x = ifelse(is.na(fourth3$ORIGINAL_BATCH.x), fourth3$ORIGINAL_BATCH.y, fourth3$ORIGINAL_BATCH.x)

# drop new column - dont need anymore
fourth3 = fourth3 %>%
  select(-c(ORIGINAL_BATCH.y))

# would repeat above steps for missing duplicate batch numbers however there are none so skip next few merges and rename dataframe to match others
sixth3 = fourth3

sixth3 = sixth3 %>%
  select(c(PARENT_CFN,DUPLICATE_BATCH, ORIGINAL_BATCH.x, original_prod_count, 
           dupe_prod_count, TRADE_ASSIGNMENT))


# adding counts to each batch per parent
#TEMPORARILY CALL DUPLICATE_BATCH RESPONSE_BATCH

colnames(sixth3) <- c("PARENT_CFN", "RESPONSE_BATCH","ORIGINAL_BATCH",
                      "original_de_count","dupe_de_count","TRADE")

# adding column with count of total core variable questions answered within each batch 
# start with adding a column with the count and then move the count into appropriate column
seven3 = left_join(sixth3,de_count, by = c("PARENT_CFN","RESPONSE_BATCH"))
# move numbers over to correct column and rename columns
seven3$dupe_de_count = ifelse(is.na(seven3$n), 0, seven3$n )
seven3 = seven3 %>%
  select(-n)

# continuing adding counts but for original now
#TEMPORARILY CALL ORIGINAL BATCH RESPONSE_ATCH
colnames(seven3) <- c("PARENT_CFN", "DUPLICATE_BATCH","RESPONSE_BATCH",
                      "OG_DE_COUNT","DUPE_DE_COUNT", "TRADE")

# move counts over to correct column and rename column
eight3 = left_join(seven3, de_count, by = c("PARENT_CFN","RESPONSE_BATCH"))

# move numbers over ot correct column and rename columns
eight3$OG_DE_COUNT = ifelse(is.na(eight3$n), 0 , eight3$n)
eight3 = eight3%>%
  select(-n)
colnames(eight3)<- c("PARENT_CFN","DUPLICATE_BATCH","ORIGINAL_BATCH",
                     "OG_DE_COUNT","DUPE_DE_COUNT","TRADE")


# COMPARE COUNTS
eight3$dupe_more_or_same = ifelse(eight3$OG_DE_COUNT<=eight3$DUPE_DE_COUNT, "yes","no")



# -------------------- counts for core data elements for not replaced indicators----------------------------------------------

# counts to use later -- number of data_elements that occur per batch
de_count1 = for_de_count_df1 %>%
  group_by(PARENT_CFN) %>%
  count(RESPONSE_BATCH)

# all unique parent_cfns for cases where at least one prod_# appeared between orignal and dupe
newtable4 = data.frame(PARENT_CFN = unique(for_de_count_df1$PARENT_CFN))

# start with placing duplicate batch numbers next to parent_cfn
formerge_dupe4 = for_dupe_de_count1_df%>%
  select(PARENT_CFN,RESPONSE_BATCH,TRADE_ASSIGNMENT)

formerge_dupe4 = unique(formerge_dupe4)


first4 = full_join(formerge_dupe4, newtable4, by = "PARENT_CFN")


# next place orignal batch numbers next to parent_cfn and duplicate batch number -- need to rename RESPONSE_BATCH to merge
formerge_original4 = for_original_de_count1_df%>%
  select(PARENT_CFN,RESPONSE_BATCH)

formerge_original4 = unique(formerge_original4)


colnames(formerge_original4) <- c("PARENT_CFN", "ORIGINAL_BATCH")
colnames(first4) <- c("PARENT_CFN","DUPLICATE_BATCH","TRADE_ASSIGNMENT")

second4 = full_join(first4,formerge_original4, by = "PARENT_CFN")


# next, need to be able to fill in batch numbers where either dupe or original had some and other had 0 core variables
# use NAs to fill response batch with 0 first - this is where one of the two batches had no core variables but the other had atleast one
# because one batch contained no data elements, its response_batch wasnt pulled and needs to be added now instead
second4[is.na(second4)] <- 0

second4$original_prod_count = ""
second4$original_prod_count = ifelse(second4$ORIGINAL_BATCH == 0, 0,NA)

second4$dupe_prod_count = ""
second4$dupe_prod_count = ifelse(second4$DUPLICATE_BATCH == 0, 0 , NA)


# pulling missing batch numbers for when original batch had no core data elements but dupe did
missing_rb4 = second4 %>%
  filter(ORIGINAL_BATCH == '0')%>%
  select(PARENT_CFN)

# repeating steps above but this time for when the duplicate batch had no core data elements but the original did
missing_rb_dupe4 = second4 %>%
  filter(DUPLICATE_BATCH == '0')%>%
  select(PARENT_CFN)



# to be able to merge - replace filled in 0s with NAs again  (onyl used 0s to be able to pull missing response batch numbers)
# change back to NAs for merge
second4$ORIGINAL_BATCH = ifelse(second4$ORIGINAL_BATCH == 0, NA, second4$ORIGINAL_BATCH)


third4 = left_join(x = missing_rb4, y = for_missing_rb_df, by = "PARENT_CFN")

# cases where no original batch exists in the BR 
# dont need no_original_batch but nice to have to look at specific weird cases ^^
# filter them out of third after
no_original_batch4 = third4 %>%
  filter(is.na(RESPONSE_BATCH))

third4 = third4 %>%
  filter(!is.na(RESPONSE_BATCH))

second4 = second4 %>%
  filter(!PARENT_CFN %in% no_original_batch4$PARENT_CFN)



colnames(third4) <- c("PARENT_CFN","ORIGINAL_BATCH")

# merge to fill in missing original batch numbers - dont merge on ORIGINAL_BATCH because not all will have original batch in second
# need to remove cases with no original batch number from 'second' df also
fourth4 = full_join(second4, third4, by = c("PARENT_CFN"))

# use new column to fill in missing response batch values
fourth4$ORIGINAL_BATCH.x = ifelse(is.na(fourth4$ORIGINAL_BATCH.x), fourth4$ORIGINAL_BATCH.y, fourth4$ORIGINAL_BATCH.x)

# drop new column - dont need anymore
fourth4 = fourth4 %>%
  select(-c(ORIGINAL_BATCH.y))

# instead of repeating above steps for missing duplicate batch numbers, drop 4 rows with missing info because didnt pull trade area
# skip fourth to fifth to match rest of code following this 
sixth4 = fourth4 %>%
  filter(!TRADE_ASSIGNMENT == '0')

# reorder columns

sixth4 = sixth4 %>%
  select(c(PARENT_CFN,DUPLICATE_BATCH, ORIGINAL_BATCH.x, original_prod_count, 
           dupe_prod_count, TRADE_ASSIGNMENT))


# adding counts to each batch per parent

#TEMPORARILY CALL DUPLICATE_BATCH RESPONSE_BATCH
colnames(sixth4) <- c("PARENT_CFN", "RESPONSE_BATCH","ORIGINAL_BATCH",
                      "original_de_count","dupe_de_count", "TRADE")

# adding column with count of total core variable questions answered within each batch 
# start with adding a column with the count and then move the count into appropriate column
seven4 = left_join(sixth4,de_count1, by = c("PARENT_CFN","RESPONSE_BATCH"))

# move numbers over to correct column and rename columns
seven4$dupe_de_count = ifelse(is.na(seven4$n), 0, seven4$n )

seven4 = seven4 %>%
  select(-n)

# continuing adding counts but for original now
# TEMPORARILY CALL ORIGINAL BATCH RESPONSE_ATCH
colnames(seven4) <- c("PARENT_CFN", "DUPLICATE_BATCH","RESPONSE_BATCH",
                      "OG_DE_COUNT","DUPE_DE_COUNT","TRADE")


eight4 = left_join(seven4, de_count1, by = c("PARENT_CFN","RESPONSE_BATCH"))

# move numbers over ot correct column and rename columns
eight4$OG_DE_COUNT = ifelse(is.na(eight4$n), 0 , eight4$n)

eight4 = eight4%>%
  select(-n)
colnames(eight4)<- c("PARENT_CFN","DUPLICATE_BATCH","ORIGINAL_BATCH",
                     "OG_DE_COUNT","DUPE_DE_COUNT","TRADE")


# COMPARE COUNTS
eight4$dupe_more_or_same = ifelse(eight4$OG_DE_COUNT<=eight4$DUPE_DE_COUNT, "yes","no")


describe(eight4$dupe_more_or_same)



################################################### table creations ########################################################

# table - total product number questions and core variable questions answered per each batch
# give 4 tables created new more useful names
# rename dupe_more_or_same variable for merge because both data element and prod counts have the comparison column named the same

prod_num_count_for_replaced = eight
colnames(prod_num_count_for_replaced) = c("PARENT_CFN","DUPLICATE_BATCH",
                                          "ORIGINAL_BATCH","OG_PROD_COUNT",
                                          "DUPE_PROD_COUNT", "TRADE","DUPE_PROD_COUNT_SAME_OR_MORE")

prod_num_count_for_not_replaced = eight1
colnames(prod_num_count_for_not_replaced) = c("PARENT_CFN","DUPLICATE_BATCH",
                                              "ORIGINAL_BATCH","OG_PROD_COUNT",
                                              "DUPE_PROD_COUNT","TRADE", "DUPE_PROD_COUNT_SAME_OR_MORE")
de_count_for_replaced = eight3
colnames(de_count_for_replaced) = c("PARENT_CFN","DUPLICATE_BATCH",
                                    "ORIGINAL_BATCH","OG_DE_COUNT",
                                    "DUPE_DE_COUNT", "TRADE", "DUPE_DE_COUNT_SAME_OR_MORE")
de_count_for_not_replaced = eight4
colnames(de_count_for_not_replaced) = c("PARENT_CFN","DUPLICATE_BATCH",
                                        "ORIGINAL_BATCH","OG_DE_COUNT",
                                        "DUPE_DE_COUNT", "TRADE", "DUPE_DE_COUNT_SAME_OR_MORE")


# merge the replaced together and merge the not replaced together?

core_and_prod_count_replaced = full_join(prod_num_count_for_replaced, de_count_for_replaced,
                                         by = c("PARENT_CFN","ORIGINAL_BATCH",
                                                "DUPLICATE_BATCH", "TRADE"))


core_and_prod_count_not_replaced = full_join(prod_num_count_for_not_replaced, de_count_for_not_replaced,
                                             by = c("PARENT_CFN","ORIGINAL_BATCH",
                                                    "DUPLICATE_BATCH", "TRADE"))


# fill in introduced NAs with 0s

# first, replaced indicators

core_and_prod_count_replaced$DUPE_PROD_COUNT = ifelse(is.na(core_and_prod_count_replaced$DUPE_PROD_COUNT), 0,
                                                      core_and_prod_count_replaced$DUPE_PROD_COUNT)

core_and_prod_count_replaced$DUPE_DE_COUNT = ifelse(is.na(core_and_prod_count_replaced$DUPE_DE_COUNT), 0, 
                                                    core_and_prod_count_replaced$DUPE_DE_COUNT)

core_and_prod_count_replaced$OG_PROD_COUNT = ifelse(is.na(core_and_prod_count_replaced$OG_PROD_COUNT), 0,
                                                    core_and_prod_count_replaced$OG_PROD_COUNT)

core_and_prod_count_replaced$OG_DE_COUNT = ifelse(is.na(core_and_prod_count_replaced$OG_DE_COUNT), 0, 
                                                  core_and_prod_count_replaced$OG_DE_COUNT)

# next, not replaced indicators

core_and_prod_count_not_replaced$DUPE_PROD_COUNT = ifelse(is.na(core_and_prod_count_not_replaced$DUPE_PROD_COUNT), 0,
                                                          core_and_prod_count_not_replaced$DUPE_PROD_COUNT)

core_and_prod_count_not_replaced$DUPE_DE_COUNT = ifelse(is.na(core_and_prod_count_not_replaced$DUPE_DE_COUNT), 0, 
                                                        core_and_prod_count_not_replaced$DUPE_DE_COUNT)

core_and_prod_count_not_replaced$OG_PROD_COUNT = ifelse(is.na(core_and_prod_count_not_replaced$OG_PROD_COUNT), 0,
                                                        core_and_prod_count_not_replaced$OG_PROD_COUNT)

core_and_prod_count_not_replaced$OG_DE_COUNT = ifelse(is.na(core_and_prod_count_not_replaced$OG_DE_COUNT), 0, 
                                                      core_and_prod_count_not_replaced$OG_DE_COUNT)


# fix dupe_more_or_same columns

core_and_prod_count_replaced$DUPE_PROD_COUNT_SAME_OR_MORE = ifelse(core_and_prod_count_replaced$DUPE_PROD_COUNT >= core_and_prod_count_replaced$OG_PROD_COUNT,
                                                                   "yes","no")

core_and_prod_count_replaced$DUPE_DE_COUNT_SAME_OR_MORE = ifelse(core_and_prod_count_replaced$DUPE_DE_COUNT >= core_and_prod_count_replaced$OG_DE_COUNT,
                                                                 "yes","no")

core_and_prod_count_not_replaced$DUPE_PROD_COUNT_SAME_OR_MORE = ifelse(core_and_prod_count_not_replaced$DUPE_PROD_COUNT >= core_and_prod_count_not_replaced$OG_PROD_COUNT,
                                                                       "yes","no")

core_and_prod_count_not_replaced$DUPE_DE_COUNT_SAME_OR_MORE = ifelse(core_and_prod_count_not_replaced$DUPE_DE_COUNT >= core_and_prod_count_not_replaced$OG_DE_COUNT,
                                                                     "yes","no")


# add both of these numbers (de and prod num counts)

#replaced indicators
core_and_prod_count_replaced$DUPE_TOTAL = core_and_prod_count_replaced$DUPE_DE_COUNT + core_and_prod_count_replaced$DUPE_PROD_COUNT
core_and_prod_count_replaced$OG_TOTAL = core_and_prod_count_replaced$OG_DE_COUNT + core_and_prod_count_replaced$OG_PROD_COUNT
core_and_prod_count_replaced$DUPE_TOTAL_MORE_OR_SAME = ifelse(core_and_prod_count_replaced$DUPE_TOTAL>=core_and_prod_count_replaced$OG_TOTAL,
                                                              "yes","no")

# not replaced indicators
core_and_prod_count_not_replaced$DUPE_TOTAL = core_and_prod_count_not_replaced$DUPE_DE_COUNT + core_and_prod_count_not_replaced$DUPE_PROD_COUNT
core_and_prod_count_not_replaced$OG_TOTAL = core_and_prod_count_not_replaced$OG_DE_COUNT + core_and_prod_count_not_replaced$OG_PROD_COUNT
core_and_prod_count_not_replaced$DUPE_TOTAL_MORE_OR_SAME = ifelse(core_and_prod_count_not_replaced$DUPE_TOTAL>=core_and_prod_count_not_replaced$OG_TOTAL,
                                                                  "yes","no")



# add time difference columns

# first need to pull date from each batch

# replaced indicators
# strip off date from end of batch
core_and_prod_count_replaced$DUPLICATE_BATCH_DATE = str_sub(core_and_prod_count_replaced$DUPLICATE_BATCH, -12,-1)
core_and_prod_count_replaced$ORIGINAL_BATCH_DATE = str_sub(core_and_prod_count_replaced$ORIGINAL_BATCH, -12,-1)
# feed R format of date
core_and_prod_count_replaced$DUPLICATE_BATCH_DATE = strptime(core_and_prod_count_replaced$DUPLICATE_BATCH_DATE,
                                                             format = "%y%m%d%H%M%S")
core_and_prod_count_replaced$ORIGINAL_BATCH_DATE = strptime(core_and_prod_count_replaced$ORIGINAL_BATCH_DAT,
                                                            format = "%y%m%d%H%M%S")
# not replaced indicators
#strip off date from end of batch name
core_and_prod_count_not_replaced$DUPLICATE_BATCH_DATE = str_sub(core_and_prod_count_not_replaced$DUPLICATE_BATCH, -12,-1)
core_and_prod_count_not_replaced$ORIGINAL_BATCH_DATE = str_sub(core_and_prod_count_not_replaced$ORIGINAL_BATCH, -12,-1)
# feed R format of date
core_and_prod_count_not_replaced$DUPLICATE_BATCH_DATE = strptime(core_and_prod_count_not_replaced$DUPLICATE_BATCH_DATE,
                                                                 format = "%y%m%d%H%M%S")
core_and_prod_count_not_replaced$ORIGINAL_BATCH_DATE = strptime(core_and_prod_count_not_replaced$ORIGINAL_BATCH_DATE,
                                                                format = "%y%m%d%H%M%S")

# next, need to calculate difference between dates

# replaced indicators

core_and_prod_count_replaced$hours_between = as.numeric(abs(difftime(core_and_prod_count_replaced$DUPLICATE_BATCH_DATE,
                                                                     core_and_prod_count_replaced$ORIGINAL_BATCH_DATE,
                                                                     units = "hours")))


# not replaced indicators

core_and_prod_count_not_replaced$hours_between = as.numeric(abs(difftime(core_and_prod_count_not_replaced$DUPLICATE_BATCH_DATE,
                                                                         core_and_prod_count_not_replaced$ORIGINAL_BATCH_DATE,
                                                                         units = "hours")))



# saving data frames as excel files
write_xlsx(core_and_prod_count_replaced,"some file path")
write_xlsx(core_and_prod_count_not_replaced,"some file path")
