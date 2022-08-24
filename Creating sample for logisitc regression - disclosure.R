
#"The Census Bureau has reviewed this data product to ensure appropriate access, use, and disclosure avoidance protection of the confidential 
# source data used to produce this product (Data Management System (DMS) number: P-7504847, subproject P-7514952, Disclosure Review Board (DRB)
# approval number: CBDRB-FY22-EWD001-007)."




############################################################################################################
# Anahi Rodriguez
# Creating table for logistic regression analysis 
# start with replaced files indicators
# next is non replaced files indicators
# 8/4/22
############################################################################################################

library(readxl)
library(DBI)
library(dbplyr)
library(dplyr)
library(tidyr)
library(writexl)
library(Hmisc)
library(writexl)
library(stringr)




#sign in

pass = rstudioapi::askForPassword("Database password")
con_string = paste0("database path here", 
                    pass)

con = dbConnect(odbc::odbc(), .connection_string = con_string, timeout = 10)


# load in data 
core_and_prod_count_replaced = read_excel("core_variables_and_prod_count_for_replaced.xlsx")
core_and_prod_count_not_replaced = read_excel("core_variables_and_prod_count_for_not_replaced.xlsx")


# need to create a table to merge with above table^ that has actual entries
# need data element entries for core variables

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

# have this data frame saved but have included the SQL query below
# check_df = read_excel("2017_PARENT_CFNS_B_BATCHES_AND_PR_BATCHES.xlsx")
# above dataframe only removes B batches

check = build_sql(
  "select distinct response_batch, parent_cfn
  from data_capture_staging
  where ref_per = '2017U1'",
  con = con)
check_df = DBI::dbGetQuery(con,check)


check_df = check_df %>%
  filter(grepl('B|0292000',RESPONSE_BATCH))




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

# ----------------------------------  TABLE CREATION --------------------------------------------

# going to use this to evaluate dollar differences against time differences
# adding in dates for day submission was sent in as well as for reminders/follow-ups to complete survey
# differences between these as well
# adding in all the responses to core variable questions as well as counts for product number questions answered



#### replaced files first

# first, remove any variables i wont need as well as rename variables for merge

for_original_de_count_df = for_original_de_count_df %>%
  mutate(OG_RESPONSE_BATCH = RESPONSE_BATCH,
         OG_ANSWER = ITEM_VALUE_ORIG)  %>%
  select(-c("REF_PER","DUPLICATE_INDICATOR",
            "REPEAT_GROUP_NUM","DUPE_TYP",
            "RESPONSE_BATCH", "DUP_COUNT",
            "ITEM_VALUE_ORIG"))

for_dupe_de_count_df = for_dupe_de_count_df %>%
  mutate(DUPE_RESPONSE_BATCH = RESPONSE_BATCH,
         DUPE_ANSWER = ITEM_VALUE_ORIG) %>%
  select(-c("REF_PER","DUPLICATE_INDICATOR",
            "REPEAT_GROUP_NUM",
            "RESPONSE_BATCH", "DUP_COUNT",
            "ITEM_VALUE_ORIG"))



# now merge these two into one table with all info across

replaced_data_elements_only = full_join(for_original_de_count_df,
                                        for_dupe_de_count_df,
                                        by = c("PARENT_CFN","CHILD_CFN",
                                               "DATA_ELEMENT","SURVUNIT_TYP"))



#reorder columns 

replaced_data_elements_only = replaced_data_elements_only %>%
  select(c("PARENT_CFN", "CHILD_CFN", "OG_RESPONSE_BATCH","DUPE_RESPONSE_BATCH",
           "DATA_ELEMENT", "OG_ANSWER", "DUPE_ANSWER", "SURVUNIT_TYP", 
           "TRADE_ASSIGNMENT.y", "DUPE_TYP"))



# add row for difference in answer
# think about how to treat differences where question was only answered in one of two batches?

replaced_data_elements_only$input_difference = as.numeric(replaced_data_elements_only$OG_ANSWER) - 
  as.numeric(replaced_data_elements_only$DUPE_ANSWER)




# lastly, need to add product counts for each batch and their differences

# to do this, need to rename columns for merge

temp_core_for_merge = core_and_prod_count_replaced %>%
  mutate(OG_RESPONSE_BATCH = ORIGINAL_BATCH,
         DUPE_RESPONSE_BATCH = DUPLICATE_BATCH,
         HOURS_BETWEEN = hours_between,
         TRADE_ASSIGNMENT = TRADE) %>%
  select(PARENT_CFN, DUPE_RESPONSE_BATCH, OG_RESPONSE_BATCH, DUPE_DE_COUNT,
         DUPE_PROD_COUNT, OG_DE_COUNT, OG_PROD_COUNT, HOURS_BETWEEN,
         ORIGINAL_BATCH_DATE, DUPLICATE_BATCH_DATE, TRADE_ASSIGNMENT)

# rename trade_assignment in temp to be able to merge
colnames(replaced_data_elements_only) = c('PARENT_CFN','CHILD_CFN','OG_RESPONSE_BATCH',
                                          'DUPE_RESPONSE_BATCH','DATA_ELEMENT','OG_ANSWER',
                                          'DUPE_ANSWER', 'SURVUNIT_TYP','TRADE_ASSIGNMENT',
                                          'DUPE_TYP','INPUT_DIFFERENCE')


# merge with replaced_data_elements_only table

replaced_data_elements_only = full_join(replaced_data_elements_only, temp_core_for_merge,
                                        by = c("PARENT_CFN","DUPE_RESPONSE_BATCH",
                                               "OG_RESPONSE_BATCH","TRADE_ASSIGNMENT"))


### not replaced files next


for_original_de_count1_df = for_original_de_count1_df %>%
  mutate(OG_RESPONSE_BATCH = RESPONSE_BATCH,
         OG_ANSWER = ITEM_VALUE_ORIG)  %>%
  select(-c("REF_PER","DUPLICATE_INDICATOR",
            "REPEAT_GROUP_NUM","DUPE_TYP",
            "RESPONSE_BATCH", "DUP_COUNT",
            "ITEM_VALUE_ORIG"))

for_dupe_de_count1_df = for_dupe_de_count1_df %>%
  mutate(DUPE_RESPONSE_BATCH = RESPONSE_BATCH,
         DUPE_ANSWER = ITEM_VALUE_ORIG) %>%
  select(-c("REF_PER","DUPLICATE_INDICATOR",
            "REPEAT_GROUP_NUM",
            "RESPONSE_BATCH", "DUP_COUNT",
            "ITEM_VALUE_ORIG"))


# now merge these two into one table with all info across

not_replaced_data_elements_only = full_join(for_original_de_count1_df,
                                            for_dupe_de_count1_df,
                                            by = c("PARENT_CFN","CHILD_CFN",
                                                   "DATA_ELEMENT","SURVUNIT_TYP"))

#reorder columns 


not_replaced_data_elements_only = not_replaced_data_elements_only %>%
  select(c("PARENT_CFN", "CHILD_CFN", "OG_RESPONSE_BATCH",
           "DUPE_RESPONSE_BATCH","DATA_ELEMENT", "OG_ANSWER",
           "DUPE_ANSWER", "SURVUNIT_TYP", "TRADE_ASSIGNMENT.y", "DUPE_TYP"))



# add row for difference in answer
# think about how to treat differences where question was only answered in one of two batches?

not_replaced_data_elements_only$input_difference = as.numeric(not_replaced_data_elements_only$OG_ANSWER) - 
  as.numeric(not_replaced_data_elements_only$DUPE_ANSWER)


# lastly, need to add product counts for each batch and their differences


# to do this, need to rename columns for merge

temp_core_for_merge1 = core_and_prod_count_not_replaced %>%
  mutate(OG_RESPONSE_BATCH = ORIGINAL_BATCH,
         DUPE_RESPONSE_BATCH = DUPLICATE_BATCH,
         HOURS_BETWEEN = hours_between,
         TRADE_ASSIGNMENT = TRADE) %>%
  select(PARENT_CFN, DUPE_RESPONSE_BATCH, OG_RESPONSE_BATCH, DUPE_DE_COUNT,
         DUPE_PROD_COUNT, OG_DE_COUNT, OG_PROD_COUNT, HOURS_BETWEEN,
         ORIGINAL_BATCH_DATE, DUPLICATE_BATCH_DATE, TRADE_ASSIGNMENT)


# rename trade_assignment in temp to be able to merge
colnames(not_replaced_data_elements_only) = c('PARENT_CFN','CHILD_CFN','OG_RESPONSE_BATCH',
                                              'DUPE_RESPONSE_BATCH','DATA_ELEMENT','OG_ANSWER',
                                              'DUPE_ANSWER', 'SURVUNIT_TYP','TRADE_ASSIGNMENT',
                                              'DUPE_TYP', 'INPUT_DIFFERENCE')

# merge with replaced_data_elements_only table

not_replaced_data_elements_only = full_join(not_replaced_data_elements_only, temp_core_for_merge1,
                                            by = c("PARENT_CFN","DUPE_RESPONSE_BATCH",
                                                   "OG_RESPONSE_BATCH", "TRADE_ASSIGNMENT"))




# remove any last minute cases that seem weird or look to have undeRgone some edit later on

# remove when child_cfn is not 0 or larger than 2000000000
# remove cases with no duplicate batch - ONLY EXIST IN REPLACED BATCHES
# remove cases with no original batch
# removing cases above so that i can correctly classify where one batch answered a question a previous batch left blank or vice versa

replaced_data_elements_only = replaced_data_elements_only %>%
  filter(CHILD_CFN==0 | CHILD_CFN > 2000000000)

not_replaced_data_elements_only = not_replaced_data_elements_only %>%
  filter(CHILD_CFN == 0 | CHILD_CFN > 2000000000)


# FOUND THIS SPECIFIC RESPONSE BATCH NUMBER THAT TENDED TO HAVE NO ORIGINAL BATCH TRACKED IN BR
for_a_check = replaced_data_elements_only %>%
  filter(is.na(OG_RESPONSE_BATCH) & 
           DUPE_RESPONSE_BATCH != 'some response batch number') %>%
  group_by(PARENT_CFN, CHILD_CFN) %>%
  count(OG_RESPONSE_BATCH)
for_a_check = for_a_check %>%
  filter(n == 4)

replaced_data_elements_only = replaced_data_elements_only %>%
  filter(!PARENT_CFN %in% for_a_check$PARENT_CFN)


# as above, for identifying which cases do not have an original batch but for not replaced files now
for_a_check1 = not_replaced_data_elements_only %>%
  filter(is.na(OG_RESPONSE_BATCH)) %>%
  group_by(PARENT_CFN, CHILD_CFN) %>%
  count(OG_RESPONSE_BATCH)

# should remove some of the cases which has no original batch
for_a_check1 = for_a_check1 %>%
  filter(n == 4)

not_replaced_data_elements_only = not_replaced_data_elements_only %>%
  filter(!PARENT_CFN %in% c('some paretn cfns here'))


# LAST THING TO FIX BEFORE COMPLETE: DATA SLIDES!

not_replaced_data_elements_only$REPLACED = 0
replaced_data_elements_only$REPLACED = 1


# combine into one data frame
combined = rbind(not_replaced_data_elements_only,replaced_data_elements_only)


combined$OG_ANSWER = as.numeric(combined$OG_ANSWER)
combined$DUPE_ANSWER = as.numeric(combined$DUPE_ANSWER)

# fix data slides
dupe_typ_r = combined %>%
  filter(DUPE_TYP == 'R')


# divide by 1000 where there exists a number 1000ish times larger than the other entered value
dupe_typ_r$OG_ANSWER = ifelse(dupe_typ_r$OG_ANSWER > dupe_typ_r$DUPE_ANSWER
                              & ((dupe_typ_r$OG_ANSWER - dupe_typ_r$DUPE_ANSWER) > 10 ),
                              dupe_typ_r$OG_ANSWER/1000, dupe_typ_r$OG_ANSWER)

# rounding after dividing by 1000
dupe_typ_r$OG_ANSWER = round(dupe_typ_r$OG_ANSWER, 0)


dupe_typ_r$DUPE_ANSWER = ifelse(dupe_typ_r$DUPE_ANSWER > dupe_typ_r$OG_ANSWER
                                & ((dupe_typ_r$DUPE_ANSWER - dupe_typ_r$OG_ANSWER) > 10 ),
                                dupe_typ_r$DUPE_ANSWER/1000, dupe_typ_r$DUPE_ANSWER)
# rounding after dividing by 1000
dupe_typ_r$DUPE_ANSWER = round(dupe_typ_r$DUPE_ANSWER, 0)


# remove old observations for dupe type R and replace with new observations
combined = combined %>%
  filter(DUPE_TYP != 'R')

combined = rbind(combined,dupe_typ_r)
all_singles = combined



# currently, have many blank or null values for observations I will use in logistic regression, so attempt to fill these values in
# need to fill in missing og batch numbers
# can fill in some numbers so long as at least one question was answered in original batch
temp_to_fill_OG_batches_missing = all_singles %>%
  select(PARENT_CFN,CHILD_CFN,OG_RESPONSE_BATCH,DUPE_RESPONSE_BATCH)
temp_to_fill_OG_batches_missing = unique(temp_to_fill_OG_batches_missing)
temp_to_fill_OG_batches_missing = temp_to_fill_OG_batches_missing %>%
  filter(!is.na(OG_RESPONSE_BATCH))
colnames(temp_to_fill_OG_batches_missing) = c('PARENT_CFN', 'CHILD_CFN',
                                              'na_ORIGINAL_BATCH', 
                                              'DUPE_RESPONSE_BATCH')

# lastly, merge to add those missing batches - this still leaves some without an OG batch number
# some  cases are when original didnt have any of the core variable questions answered but dupe did
all_singles = full_join(all_singles,temp_to_fill_OG_batches_missing, 
                        by = c('PARENT_CFN','CHILD_CFN','DUPE_RESPONSE_BATCH'))

all_singles$OG_RESPONSE_BATCH = ifelse(is.na(all_singles$OG_RESPONSE_BATCH), 
                                       all_singles$na_ORIGINAL_BATCH,
                                       all_singles$OG_RESPONSE_BATCH)
all_singles = all_singles %>%
  select(-na_ORIGINAL_BATCH)

howmany = all_singles %>%
  filter(is.na(OG_RESPONSE_BATCH))

# atempt to fill in remaining some missing original batch numbers
check2 = build_sql(
  "select distinct response_batch, parent_cfn, child_cfn
  from Table_B
  where ref_per = '2017U1' and
  (response_batch like 'K%' or response_batch like 'G%')
  and dupe_typ is null",
  con = con)
check2 = DBI::dbGetQuery(con,check2)


check2 = check2 %>%
  filter(PARENT_CFN %in% howmany$PARENT_CFN)


all_singles = full_join(check2,all_singles, by = c("PARENT_CFN","CHILD_CFN"))
# ^^ merge introduced blank duplicate batches for parentcfns that didnt have any core variable questions answered between either batch so drop
all_singles = all_singles %>%
  filter(!is.na(DUPE_RESPONSE_BATCH))


# move over batch numbers to correct column and drop other
all_singles$OG_RESPONSE_BATCH = ifelse(is.na(all_singles$OG_RESPONSE_BATCH), all_singles$RESPONSE_BATCH, all_singles$OG_RESPONSE_BATCH)
all_singles = all_singles %>%
  select(-RESPONSE_BATCH)

# how many remain blank? - some remain blank BUT there is no original batch in BR staging table so drop
check4 = all_singles %>%
  filter(is.na(OG_RESPONSE_BATCH))

all_singles = all_singles %>%
  filter(!is.na(OG_RESPONSE_BATCH))


# lastly, with new OG batches added, need to grab their core variable questions answered count
# also need to grab their product number and NAICS code questions answered count
replaced_de_prod = core_and_prod_count_replaced %>%
  mutate(OG_RESPONSE_BATCH = ORIGINAL_BATCH,
         DUPE_RESPONSE_BATCH = DUPLICATE_BATCH) %>%
  select(c(PARENT_CFN, DUPE_RESPONSE_BATCH, OG_RESPONSE_BATCH,
           OG_PROD_COUNT, DUPE_PROD_COUNT, OG_DE_COUNT,
           DUPE_DE_COUNT))
not_replaced_de_prod = core_and_prod_count_not_replaced %>%
  mutate(OG_RESPONSE_BATCH = ORIGINAL_BATCH,
         DUPE_RESPONSE_BATCH = DUPLICATE_BATCH) %>%
  select(c(PARENT_CFN, DUPE_RESPONSE_BATCH, OG_RESPONSE_BATCH,
           OG_PROD_COUNT, DUPE_PROD_COUNT, OG_DE_COUNT,
           DUPE_DE_COUNT))




all_singles = full_join(all_singles,replaced_de_prod, by = c('PARENT_CFN', 'DUPE_RESPONSE_BATCH',
                                                             'OG_RESPONSE_BATCH'))
all_singles$DUPE_DE_COUNT.x = ifelse(is.na(all_singles$DUPE_DE_COUNT.x),all_singles$DUPE_DE_COUNT.y,all_singles$DUPE_DE_COUNT.x)
all_singles$DUPE_PROD_COUNT.x = ifelse(is.na(all_singles$DUPE_PROD_COUNT.x),all_singles$DUPE_PROD_COUNT.y,all_singles$DUPE_PROD_COUNT.x)
all_singles$OG_DE_COUNT.x = ifelse(is.na(all_singles$OG_DE_COUNT.x), all_singles$OG_DE_COUNT.y, all_singles$OG_DE_COUNT.x)
all_singles$OG_PROD_COUNT.x = ifelse(is.na(all_singles$OG_PROD_COUNT.x), all_singles$OG_PROD_COUNT.y, all_singles$OG_PROD_COUNT.x)

all_singles = all_singles %>%
  select(-c(DUPE_DE_COUNT.y, DUPE_PROD_COUNT.y, OG_DE_COUNT.y, OG_PROD_COUNT.y))


all_singles = full_join(all_singles,not_replaced_de_prod, by = c('PARENT_CFN', 'DUPE_RESPONSE_BATCH',
                                                                 'OG_RESPONSE_BATCH'))
all_singles$DUPE_DE_COUNT.x = ifelse(is.na(all_singles$DUPE_DE_COUNT.x),all_singles$DUPE_DE_COUNT,all_singles$DUPE_DE_COUNT.x)
all_singles$DUPE_PROD_COUNT.x = ifelse(is.na(all_singles$DUPE_PROD_COUNT.x),all_singles$DUPE_PROD_COUNT,all_singles$DUPE_PROD_COUNT.x)
all_singles$OG_DE_COUNT.x = ifelse(is.na(all_singles$OG_DE_COUNT.x), all_singles$OG_DE_COUNT, all_singles$OG_DE_COUNT.x)
all_singles$OG_PROD_COUNT.x = ifelse(is.na(all_singles$OG_PROD_COUNT.x), all_singles$OG_PROD_COUNT, all_singles$OG_PROD_COUNT.x)

all_singles = all_singles %>%
  select(-c(DUPE_DE_COUNT, DUPE_PROD_COUNT, OG_DE_COUNT,OG_PROD_COUNT))



# NEED TO ADD A FEW DATE AND DIFFERENCE VARIABLES AND THEN RECHECK FOR MISSINGNESS

# date variables to be added - ONLY SOME BATCH DATES LOADED FROM PREVIOUS FILES SO FIX
all_singles$DUPLICATE_BATCH_DATE = str_sub(all_singles$DUPE_RESPONSE_BATCH, -12, -1)
all_singles$ORIGINAL_BATCH_DATE = str_sub(all_singles$OG_RESPONSE_BATCH, -12, -1)

all_singles$DUPLICATE_BATCH_DATE = strptime(all_singles$DUPLICATE_BATCH_DATE,
                                            format = "%y%m%d%H%M%S")
all_singles$ORIGINAL_BATCH_DATE = strptime(all_singles$ORIGINAL_BATCH_DATE,
                                           format = "%y%m%d%H%M%S")

all_singles$HOURS_BETWEEN = as.numeric((difftime(all_singles$DUPLICATE_BATCH_DATE,
                                                 all_singles$ORIGINAL_BATCH_DATE,
                                                 units = "hours")))


# add follow up reminder dates and how long after that each batch was submitted

# due date
all_singles$DUE_DATE = "180612"
all_singles$DUE_DATE = strptime(all_singles$DUE_DATE,
                                format = "%y%m%d")
# now for days past due date
all_singles$DUPE_HOURS_PAST_DUE_DATE = as.numeric((difftime(all_singles$DUPLICATE_BATCH_DATE,
                                                            all_singles$DUE_DATE,
                                                            units = "hours")))


# 1st follow up
all_singles$FOLLOW_UP_1ST = "180720"
all_singles$FOLLOW_UP_1ST = strptime(all_singles$FOLLOW_UP_1ST,
                                     format = "%y%m%d")
# now for days past first follow up
all_singles$DUPE_HOURS_PAST_1ST = as.numeric((difftime(all_singles$DUPLICATE_BATCH_DATE,
                                                       all_singles$FOLLOW_UP_1ST,
                                                       units = "hours")))


# 2nd follow up
all_singles$FOLLOW_UP_2ND = "180831"
all_singles$FOLLOW_UP_2ND = strptime(all_singles$FOLLOW_UP_2ND,
                                     format = "%y%m%d")
# now days past second follow up
all_singles$DUPE_HOURS_PAST_2ND = as.numeric((difftime(all_singles$DUPLICATE_BATCH_DATE,
                                                       all_singles$FOLLOW_UP_2ND,
                                                       units = "hours")))

# mail follow up
all_singles$MAIL_FOLLOW_UP = "181022"
all_singles$MAIL_FOLLOW_UP = strptime(all_singles$MAIL_FOLLOW_UP,
                                      format = "%y%m%d")
# now for days past mait out follow up
all_singles$DUPE_HOURS_PAST_MAIL_FOLLOW_UP = as.numeric((difftime(all_singles$DUPLICATE_BATCH_DATE,
                                                                  all_singles$MAIL_FOLLOW_UP,
                                                                  units = "hours")))


# add in one variable that better accounts input differences to core variable questions for cases where the original had a 0 entered
# 0 for original batch might indicate that they entered in junk data and that's why their submitting a duplicate which should push the likelihood of taking the dupe
all_singles$og_answered_0 = ifelse(all_singles$OG_ANSWER == 0, 1,0)

# add in interaction term to account for when original batch had a blank - do this to keep that observation from dropping
# replace values originally not answered with 0s
# interaction term will equal 1 when there was a blank for that question

all_singles$OG_BLANK_DUMMY = ifelse(is.na(all_singles$OG_ANSWER),1,0)
all_singles$OG_ANSWER = ifelse(all_singles$OG_BLANK_DUMMY == 1, 0, all_singles$OG_ANSWER)


# now that those rows have answers for the original submission, re run the difference between batch answers 
# input difference is a variable used in regressions so cant be missing or will get dropped
all_singles$INPUT_DIFFERENCE = all_singles$DUPE_ANSWER - all_singles$OG_ANSWER
# create interaction term
all_singles$blank_interaction = all_singles$INPUT_DIFFERENCE * all_singles$OG_BLANK_DUMMY


# need to add a few more missing variables to use in regression 
# difference in prod count and difference in core variable count
all_singles$PROD_COUNT_DIF = all_singles$DUPE_PROD_COUNT - all_singles$OG_PROD_COUNT
all_singles$DE_COUNT_DIF = all_singles$DUPE_DE_COUNT - all_singles$OG_DE_COUNT

# lastly, drop some cases with missingness and move on to models
all_singles = all_singles %>%
  filter(!is.na(REPLACED))


# drop variables we dont need and rename and then save for future model usage
all_singles = rename(all_singles, DUPE_DE_COUNT = DUPE_DE_COUNT.x,
                     DUPE_PROD_COUNT = DUPE_PROD_COUNT.x,
                     OG_DE_COUNT = OG_DE_COUNT.x,
                     OG_PROD_COUNT = OG_PROD_COUNT.x,
                     OG_ANSWERED_0 = og_answered_0,
                     BLANK_0_INTERACTION = blank_interaction)



# saving data frame as excel file
write_xlsx(all_singles,"some path here")



