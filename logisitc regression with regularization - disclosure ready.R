############################################################################################################
# Anahi Rodriguez
# regularizing logistic regression model
# 3 attempts at crating a model with regularization: with weights, with oversampling, and with undersampling
# 8/8/22
############################################################################################################

library(readxl)
library(dbplyr)
library(dplyr)
library(tidyr)
library(caret)
library(caTools)
library(pROC)
library(ROCR)
library(pscl)
library(glmnet)
library(randomForest)
library(ROSE)
library(Hmisc)
library(plotmo)




# load in necessary functions
######  setwd('some path here')
source("Function File for Models.R")

# load in data sets
### setwd("some path here")
#all_singles = read_excel("for_logistic_regression_sample") #- currently the sample that would be produced from script but something is wrong
all_singles = read_excel("all_single_dupes_sample.xlsx") # i have an old copy of the sample I want saved luckily



all_singles = all_singles %>%
  filter(grepl('C|F|M|N|R|S|U|W' ,TRADE_ASSIGNMENT))
all_singles$REPLACED = as.factor(all_singles$REPLACED)



# split data 
set.seed(4)
sample2 <- sample.split(all_singles$REPLACED, SplitRatio = 0.8)
train2  <- subset(all_singles, sample2 == TRUE)
test2   <- subset(all_singles, sample2 == FALSE)


describe(train2$REPLACED)
describe(all_singles$REPLACED)
describe(test2$REPLACED)
describe(all_singles$TRADE_ASSIGNMENT)
describe(train2$TRADE_ASSIGNMENT)
describe(test2$TRADE_ASSIGNMENT)



# double check proportions - 80/20
nrow(train2)/nrow(all_singles)
nrow(test2)/nrow(all_singles)



# normalizing training data - 
train2 <- train2 %>% 
  mutate(HOURS_BETWEEN = scale(HOURS_BETWEEN),
         INPUT_DIFFERENCE = scale(INPUT_DIFFERENCE),
         BLANK_0_INTERACTION = scale(BLANK_0_INTERACTION),
         DUPE_HOURS_PAST_DUE_DATE = scale(DUPE_HOURS_PAST_DUE_DATE),
         DUPE_HOURS_PAST_1ST = scale(DUPE_HOURS_PAST_1ST),
         DUPE_HOURS_PAST_2ND = scale(DUPE_HOURS_PAST_2ND),
         DUPE_HOURS_PAST_MAIL_FOLLOW_UP = scale(DUPE_HOURS_PAST_MAIL_FOLLOW_UP),
         PROD_COUNT_DIF = scale(PROD_COUNT_DIF),DE_COUNT_DIF = scale(DE_COUNT_DIF))

# create a design matrix for both the training data and test data

# make matrix of independent variables for training data
# categorical variables
REPLACED_train = train2$REPLACED
SURVUNIT_TYP_train = as.factor(train2$SURVUNIT_TYP)
TRADE_ASSIGNMENT_train = as.factor(train2$TRADE_ASSIGNMENT)
OG_BLANK_DUMMY_train = as.factor(train2$OG_BLANK_DUMMY)
# continous variables
OG_BLANK_INPUT_INTERACTION_train = (train2$BLANK_0_INTERACTION)
HOURS_BETWEEN_train = train2$HOURS_BETWEEN
INPUT_DIFFERENCE_train = train2$INPUT_DIFFERENCE
DUPE_HOURS_PAST_DUE_DATE_train = train2$DUPE_HOURS_PAST_DUE_DATE
DUPE_HOURS_PAST_1ST_train = train2$DUPE_HOURS_PAST_1ST
DUPE_HOURS_PAST_2ND_train = train2$DUPE_HOURS_PAST_2ND
DUPE_HOURS_PAST_MAIL_FOLLOW_UP_train = train2$DUPE_HOURS_PAST_MAIL_FOLLOW_UP
PROD_COUNT_DIF_train = train2$PROD_COUNT_DIF
DE_COUNT_DIF_train = train2$DE_COUNT_DIF


x.factors_train = model.matrix(REPLACED_train ~ SURVUNIT_TYP_train + TRADE_ASSIGNMENT_train + 
                                 OG_BLANK_DUMMY_train )[,-1]
x_train = as.matrix(data.frame(HOURS_BETWEEN_train,INPUT_DIFFERENCE_train,
                               OG_BLANK_INPUT_INTERACTION_train,
                               DUPE_HOURS_PAST_1ST_train, DUPE_HOURS_PAST_2ND_train,
                               DUPE_HOURS_PAST_DUE_DATE_train,
                               DUPE_HOURS_PAST_MAIL_FOLLOW_UP_train,
                               PROD_COUNT_DIF_train, DE_COUNT_DIF_train,
                               x.factors_train))



# make matrix of independent variables for test data
# categorical variables
REPLACED = test2$REPLACED
SURVUNIT_TYP = as.factor(test2$SURVUNIT_TYP)
TRADE_ASSIGNMENT = as.factor(test2$TRADE_ASSIGNMENT)
OG_BLANK_DUMMY = as.factor(test2$OG_BLANK_DUMMY)
# continous variables
OG_BLANK_INPUT_INTERACTION = (test2$BLANK_0_INTERACTION)
HOURS_BETWEEN = test2$HOURS_BETWEEN
INPUT_DIFFERENCE = test2$INPUT_DIFFERENCE
DUPE_HOURS_PAST_DUE_DATE = test2$DUPE_HOURS_PAST_DUE_DATE
DUPE_HOURS_PAST_1ST = test2$DUPE_HOURS_PAST_1ST
DUPE_HOURS_PAST_2ND = test2$DUPE_HOURS_PAST_2ND
DUPE_HOURS_PAST_MAIL_FOLLOW_UP = test2$DUPE_HOURS_PAST_MAIL_FOLLOW_UP
PROD_COUNT_DIF = test2$PROD_COUNT_DIF
DE_COUNT_DIF = test2$DE_COUNT_DIF


x.factors_test2 = model.matrix(REPLACED ~ SURVUNIT_TYP + TRADE_ASSIGNMENT + 
                                 OG_BLANK_DUMMY )[,-1]
x_test = as.matrix(data.frame(HOURS_BETWEEN,INPUT_DIFFERENCE,DUPE_HOURS_PAST_1ST,
                              OG_BLANK_INPUT_INTERACTION,
                              DUPE_HOURS_PAST_2ND, DUPE_HOURS_PAST_DUE_DATE,
                              DUPE_HOURS_PAST_MAIL_FOLLOW_UP, PROD_COUNT_DIF, 
                              DE_COUNT_DIF, x.factors_test2))

# add weights to training data
proportion_1 = (length((train2$REPLACED)[train2$REPLACED == 1]))/(length(train2$REPLACED))
train2$weights = ifelse(train2$REPLACED == 0, proportion_1, (1 - proportion_1))
describe(train2$weights)


# create a model using training data

fit <- glmnet(x_train, y=(REPLACED_train), weights = train2$weights, family="binomial")


# labels only the top 30 largest coefficients - i have much less so will label all
plot_glmnet(fit, label=30)
# plot gives same lines with different top axis label
plot(fit, xvar = "lambda", label = TRUE)

# cross validation to find the best lambda value
cvfit = cv.glmnet(x_train, y = (REPLACED_train), weights = train2$weights,
                  family = "binomial", type = "class")
plot(cvfit)

# following are lambda value at which smallest MSE is achieved 
# and the largest lambda at which MSE is within one standard error of the smallest MSE
# both are the same
# MSE = mean squared error

cvfit$lambda.min
cvfit$lambda.1se



# predictions on the test set
predictions <- predict(fit, newx = x_test, type = 'response', s= cvfit$lambda.min)
predictions_cv = predict(cvfit, newx = x_test, type = 'response', s = cvfit$lambda.min)
predict(fit,type="coef", s = cvfit$lambda.min)
summary(predictions)


# assessing fit - following two should be same
assess.glmnet(fit, newx = x_test, newy = REPLACED, family = "binomial", s= cvfit$lambda.min)
assess.glmnet(cvfit, newx = x_test, newy = REPLACED, family = "binomial", s= cvfit$lambda.min)





# plotting ROC curve and finding AUC
# use this plot to better predict where cutoff would be best 
for_roc_auc_threshold(predictions,REPLACED)



# trying to fix imbalance in data

#### over sampling
# attempt to over-sample not replaced cases - cases where we kept the original to match a more 50/50 split

# split data 
set.seed(5)
sample2 <- sample.split(all_singles$REPLACED, SplitRatio = 0.8)
train2  <- subset(all_singles, sample2 == TRUE)
test2   <- subset(all_singles, sample2 == FALSE)



# attempt to over-sample not replaced cases - cases where we kept the original to match a more 50/50 split
over <- ovun.sample(REPLACED ~HOURS_BETWEEN + SURVUNIT_TYP + TRADE_ASSIGNMENT + 
                      INPUT_DIFFERENCE + OG_BLANK_DUMMY + BLANK_0_INTERACTION + 
                      DUPE_HOURS_PAST_DUE_DATE + DUPE_HOURS_PAST_1ST + 
                      DUPE_HOURS_PAST_2ND + DUPE_HOURS_PAST_MAIL_FOLLOW_UP + 
                      PROD_COUNT_DIF + DE_COUNT_DIF, data = train2,
                    method = "over", p=0.5, seed = 2)$data

describe(over$REPLACED)
describe(over$TRADE_ASSIGNMENT)


# normalizing training data - WILL NOW USE OVER INSTEAD OF TRAIN2
over <- over %>% 
  mutate(HOURS_BETWEEN = scale(HOURS_BETWEEN),
         INPUT_DIFFERENCE = scale(INPUT_DIFFERENCE),
         BLANK_0_INTERACTION = scale(BLANK_0_INTERACTION),
         DUPE_HOURS_PAST_DUE_DATE = scale(DUPE_HOURS_PAST_DUE_DATE),
         DUPE_HOURS_PAST_1ST = scale(DUPE_HOURS_PAST_1ST),
         DUPE_HOURS_PAST_2ND = scale(DUPE_HOURS_PAST_2ND),
         DUPE_HOURS_PAST_MAIL_FOLLOW_UP = scale(DUPE_HOURS_PAST_MAIL_FOLLOW_UP),
         PROD_COUNT_DIF = scale(PROD_COUNT_DIF),DE_COUNT_DIF = scale(DE_COUNT_DIF))

# create a design matrix for both the training data (over_new) and the test data (test2)

# make matrix of independent variables for training data
# categorical variables
REPLACED_train = over$REPLACED
SURVUNIT_TYP_train = as.factor(over$SURVUNIT_TYP)
TRADE_ASSIGNMENT_train = as.factor(over$TRADE_ASSIGNMENT)
OG_BLANK_DUMMY_train = as.factor(over$OG_BLANK_DUMMY)
# continous variables
OG_BLANK_INPUT_INTERACTION_train = (over$BLANK_0_INTERACTION)
HOURS_BETWEEN_train = over$HOURS_BETWEEN
INPUT_DIFFERENCE_train = over$INPUT_DIFFERENCE
DUPE_HOURS_PAST_DUE_DATE_train = over$DUPE_HOURS_PAST_DUE_DATE
DUPE_HOURS_PAST_1ST_train = over$DUPE_HOURS_PAST_1ST
DUPE_HOURS_PAST_2ND_train = over$DUPE_HOURS_PAST_2ND
DUPE_HOURS_PAST_MAIL_FOLLOW_UP_train = over$DUPE_HOURS_PAST_MAIL_FOLLOW_UP
PROD_COUNT_DIF_train = over$PROD_COUNT_DIF
DE_COUNT_DIF_train = over$DE_COUNT_DIF


x.factors_train = model.matrix(REPLACED_train ~ SURVUNIT_TYP_train + TRADE_ASSIGNMENT_train + 
                                 OG_BLANK_DUMMY_train )[,-1]
x_train = as.matrix(data.frame(HOURS_BETWEEN_train,INPUT_DIFFERENCE_train,
                               OG_BLANK_INPUT_INTERACTION_train,
                               DUPE_HOURS_PAST_1ST_train, DUPE_HOURS_PAST_2ND_train,
                               DUPE_HOURS_PAST_DUE_DATE_train,
                               DUPE_HOURS_PAST_MAIL_FOLLOW_UP_train,
                               PROD_COUNT_DIF_train, DE_COUNT_DIF_train,
                               x.factors_train))


# make matrix of independent variables for test data
# categorical variables
REPLACED = test2$REPLACED
SURVUNIT_TYP = as.factor(test2$SURVUNIT_TYP)
TRADE_ASSIGNMENT = as.factor(test2$TRADE_ASSIGNMENT)
OG_BLANK_DUMMY = as.factor(test2$OG_BLANK_DUMMY)
# continous variables
OG_BLANK_INPUT_INTERACTION = (test2$BLANK_0_INTERACTION)
HOURS_BETWEEN = test2$HOURS_BETWEEN
INPUT_DIFFERENCE = test2$INPUT_DIFFERENCE
DUPE_HOURS_PAST_DUE_DATE = test2$DUPE_HOURS_PAST_DUE_DATE
DUPE_HOURS_PAST_1ST = test2$DUPE_HOURS_PAST_1ST
DUPE_HOURS_PAST_2ND = test2$DUPE_HOURS_PAST_2ND
DUPE_HOURS_PAST_MAIL_FOLLOW_UP = test2$DUPE_HOURS_PAST_MAIL_FOLLOW_UP
PROD_COUNT_DIF = test2$PROD_COUNT_DIF
DE_COUNT_DIF = test2$DE_COUNT_DIF


x.factors_test2 = model.matrix(REPLACED ~ SURVUNIT_TYP + TRADE_ASSIGNMENT + 
                                 OG_BLANK_DUMMY )[,-1]
x_test = as.matrix(data.frame(HOURS_BETWEEN,INPUT_DIFFERENCE,DUPE_HOURS_PAST_1ST,
                              OG_BLANK_INPUT_INTERACTION,
                              DUPE_HOURS_PAST_2ND, DUPE_HOURS_PAST_DUE_DATE,
                              DUPE_HOURS_PAST_MAIL_FOLLOW_UP, PROD_COUNT_DIF, 
                              DE_COUNT_DIF, x.factors_test2))



# create a model using training data

fit <- glmnet(x_train, y=(REPLACED_train), family="binomial")


# labels only the top 30 largest coefficients - i have much less so will label all
plot_glmnet(fit, label=30)
# plot gives same lines with different top axis label
plot(fit, xvar = "lambda", label = TRUE)

# cross validation to find the best lambda value
cvfit = cv.glmnet(x_train, y = (REPLACED_train),
                  family = "binomial", type = "class")
plot(cvfit)


# following are lambda value at which smallest MSE is achieved 
# and the largest lambda at which MSE is within one standard error of the smallest MSE
# both are the same
# MSE = mean squared error

log(cvfit$lambda.min)
log(cvfit$lambda.1se)



# predictions on the test set
predictions <- predict(fit, newx = x_test, type = 'response', s= cvfit$lambda.min)
predictions_cv = predict(cvfit, newx = x_test, type = 'response', s = cvfit$lambda.min)
predict(fit,type="coef", s = cvfit$lambda.min)
summary(predictions)


# assesing fit
assess.glmnet(fit, newx = x_test, newy = REPLACED, family = "binomial", s= cvfit$lambda.min)
assess.glmnet(cvfit, newx = x_test, newy = REPLACED, family = "binomial", s= cvfit$lambda.min)



# plotting ROC curve and finding AUC
# use this plot to better predict where cutoff would be best 
for_roc_auc_threshold(predictions, REPLACED)


# instead, undersample cases where we decided to keep the duplicate to try and make a moe 50/50 class split


# split data 
set.seed(6)
sample2 <- sample.split(all_singles$REPLACED, SplitRatio = 0.8)
train2  <- subset(all_singles, sample2 == TRUE)
test2   <- subset(all_singles, sample2 == FALSE)



# attempt to over-sample not replaced cases - cases where we kept the original to match a more 50/50 split
under <- ovun.sample(REPLACED ~HOURS_BETWEEN + SURVUNIT_TYP + TRADE_ASSIGNMENT + 
                       INPUT_DIFFERENCE + OG_BLANK_DUMMY + BLANK_0_INTERACTION + 
                       DUPE_HOURS_PAST_DUE_DATE + DUPE_HOURS_PAST_1ST + 
                       DUPE_HOURS_PAST_2ND + DUPE_HOURS_PAST_MAIL_FOLLOW_UP + 
                       PROD_COUNT_DIF + DE_COUNT_DIF, data = train2,
                     method = "under", p=0.5, seed = 2)$data

describe(under$REPLACED)
describe(under$TRADE_ASSIGNMENT)


# normalizing training data - WILL NOW USE OVER INSTEAD OF TRAIN2
under <- under %>% 
  mutate(HOURS_BETWEEN = scale(HOURS_BETWEEN),
         INPUT_DIFFERENCE = scale(INPUT_DIFFERENCE),
         BLANK_0_INTERACTION = scale(BLANK_0_INTERACTION),
         DUPE_HOURS_PAST_DUE_DATE = scale(DUPE_HOURS_PAST_DUE_DATE),
         DUPE_HOURS_PAST_1ST = scale(DUPE_HOURS_PAST_1ST),
         DUPE_HOURS_PAST_2ND = scale(DUPE_HOURS_PAST_2ND),
         DUPE_HOURS_PAST_MAIL_FOLLOW_UP = scale(DUPE_HOURS_PAST_MAIL_FOLLOW_UP),
         PROD_COUNT_DIF = scale(PROD_COUNT_DIF),DE_COUNT_DIF = scale(DE_COUNT_DIF))

# create a design matrix for both the training data (over_new) and the test data (test2)

# make matrix of independent variables for training data
# categorical variables
REPLACED_train = under$REPLACED
SURVUNIT_TYP_train = as.factor(under$SURVUNIT_TYP)
TRADE_ASSIGNMENT_train = as.factor(under$TRADE_ASSIGNMENT)
OG_BLANK_DUMMY_train = as.factor(under$OG_BLANK_DUMMY)
# continous variables
OG_BLANK_INPUT_INTERACTION_train = (under$BLANK_0_INTERACTION)
HOURS_BETWEEN_train = under$HOURS_BETWEEN
INPUT_DIFFERENCE_train = under$INPUT_DIFFERENCE
DUPE_HOURS_PAST_DUE_DATE_train = under$DUPE_HOURS_PAST_DUE_DATE
DUPE_HOURS_PAST_1ST_train = under$DUPE_HOURS_PAST_1ST
DUPE_HOURS_PAST_2ND_train = under$DUPE_HOURS_PAST_2ND
DUPE_HOURS_PAST_MAIL_FOLLOW_UP_train = under$DUPE_HOURS_PAST_MAIL_FOLLOW_UP
PROD_COUNT_DIF_train = under$PROD_COUNT_DIF
DE_COUNT_DIF_train = under$DE_COUNT_DIF


x.factors_train = model.matrix(REPLACED_train ~ SURVUNIT_TYP_train + TRADE_ASSIGNMENT_train + 
                                 OG_BLANK_DUMMY_train )[,-1]
x_train = as.matrix(data.frame(HOURS_BETWEEN_train,INPUT_DIFFERENCE_train,
                               OG_BLANK_INPUT_INTERACTION_train,
                               DUPE_HOURS_PAST_1ST_train, DUPE_HOURS_PAST_2ND_train,
                               DUPE_HOURS_PAST_DUE_DATE_train,
                               DUPE_HOURS_PAST_MAIL_FOLLOW_UP_train,
                               PROD_COUNT_DIF_train, DE_COUNT_DIF_train,
                               x.factors_train))


# make matrix of independent variables for test data
# categorical variables
REPLACED = test2$REPLACED
SURVUNIT_TYP = as.factor(test2$SURVUNIT_TYP)
TRADE_ASSIGNMENT = as.factor(test2$TRADE_ASSIGNMENT)
OG_BLANK_DUMMY = as.factor(test2$OG_BLANK_DUMMY)
# continous variables
OG_BLANK_INPUT_INTERACTION = (test2$BLANK_0_INTERACTION)
HOURS_BETWEEN = test2$HOURS_BETWEEN
INPUT_DIFFERENCE = test2$INPUT_DIFFERENCE
DUPE_HOURS_PAST_DUE_DATE = test2$DUPE_HOURS_PAST_DUE_DATE
DUPE_HOURS_PAST_1ST = test2$DUPE_HOURS_PAST_1ST
DUPE_HOURS_PAST_2ND = test2$DUPE_HOURS_PAST_2ND
DUPE_HOURS_PAST_MAIL_FOLLOW_UP = test2$DUPE_HOURS_PAST_MAIL_FOLLOW_UP
PROD_COUNT_DIF = test2$PROD_COUNT_DIF
DE_COUNT_DIF = test2$DE_COUNT_DIF


x.factors_test2 = model.matrix(REPLACED ~ SURVUNIT_TYP + TRADE_ASSIGNMENT + 
                                 OG_BLANK_DUMMY )[,-1]
x_test = as.matrix(data.frame(HOURS_BETWEEN,INPUT_DIFFERENCE,DUPE_HOURS_PAST_1ST,
                              OG_BLANK_INPUT_INTERACTION,
                              DUPE_HOURS_PAST_2ND, DUPE_HOURS_PAST_DUE_DATE,
                              DUPE_HOURS_PAST_MAIL_FOLLOW_UP, PROD_COUNT_DIF, 
                              DE_COUNT_DIF, x.factors_test2))



# create a model using training data

fit <- glmnet(x_train, y=(REPLACED_train), family="binomial")


# labels only the top 30 largest coefficients - i have much less so will label all
plot_glmnet(fit, label=30)
# plot gives same lines with different top axis label
plot(fit, xvar = "lambda", label = TRUE)

# cross validation to find the best lambda value
cvfit = cv.glmnet(x_train, y = (REPLACED_train),
                  family = "binomial", type = "class")
plot(cvfit)


# following are lambda value at which smallest MSE is achieved 
# and the largest lambda at which MSE is within one standard error of the smallest MSE
# both are the same
# MSE = mean squared error

log(cvfit$lambda.min)
log(cvfit$lambda.1se)



# predictions on the test set
predictions <- predict(fit, newx = x_test, type = 'response', s= cvfit$lambda.min)
predictions_cv = predict(cvfit, newx = x_test, type = 'response', s = cvfit$lambda.min)
predict(fit,type="coef", s = cvfit$lambda.min)
summary(predictions)


# assesing fit
assess.glmnet(fit, newx = x_test, newy = REPLACED, family = "binomial", s= cvfit$lambda.min)
assess.glmnet(cvfit, newx = x_test, newy = REPLACED, family = "binomial", s= cvfit$lambda.min)



# plotting ROC curve and finding AUC
# use this plot to better predict where cutoff would be best 
for_roc_auc_threshold(predictions, REPLACED)
