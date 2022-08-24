
#"The Census Bureau has reviewed this data product to ensure appropriate access, use, and disclosure avoidance protection of the confidential 
# source data used to produce this product (Data Management System (DMS) number: P-7504847, subproject P-7514952, Disclosure Review Board (DRB)
# approval number: CBDRB-FY22-EWD001-007)."

###############################################################################
# Anahi Rodirguez
# Creating Functions to use for models
# created accuracy metric functions for confusion matrix
# created a function for outputting an accuracy plot with different thresholds,ROC/AUC curve and values, and accuracy at best threshold
# created a function that would create a confusion matrix of dollar amounts for specified data_element
# created function that would spit out best AUC for each trade when all trades are put into one model
# created a function that will regularize a sample - intended to be used with each trade
# updated on 8/12/22
##############################################################################



# create functions for confusion matrix - accuracy metrics
accuracy <- function(confusion_matrix){
  print((confusion_matrix[1]  + confusion_matrix[4])/(sum(confusion_matrix[1:4])))
}

sensitivity <- function(confusion_matrix){
  print((confusion_matrix[4] / (confusion_matrix[4] + confusion_matrix[2])))
}

specificity <- function(confusion_matrix){
  print((confusion_matrix[1]/(confusion_matrix[1] + confusion_matrix[3])))
}


# function gives best threshold for highest accuracy and the ROC curve with the AUC value
for_roc_auc_threshold <- function(predictions, real_values){
  # use this plot to better predict where cutoff would be best
  pred = prediction(predictions, real_values)
  perf = performance(pred, "acc")
  print(plot(perf))

# tells us most accurate cutoff point
  max_ind = which.max(slot(perf, "y.values")[[1]] )
  acc = slot(perf, "y.values")[[1]][max_ind]
  cutoff = slot(perf, "x.values")[[1]][max_ind]
  print(c(accuracy= acc, cutoff = cutoff))
  
# print ROC curve and AUC value
  roc = performance(pred,"tpr","fpr")
  print(plot(roc, colorize = F, lwd = 2))
  abline(a = 0, b = 1)
  auc = performance(pred, measure = "auc")
  print(auc@y.values)
}




# this function will print out the total aggregated annual payroll/revenue for true/false negatives and positives
# intended for use with one model containing all trades
# negative: kept the original submission
# positive: kept the dupicate submission
for_dollar_confusion_matrix <- function(test, threshold, pred_test, data_element){
  test = test %>%
    mutate(pred = pred_test,
           model_pred_rep = ifelse(pred >= threshold, 1,0),
           tp = ifelse((model_pred_rep == 1 & REPLACED == 1), 1, 0),
           tn = ifelse((model_pred_rep == 0 & REPLACED == 0), 1, 0),
           fp = ifelse((model_pred_rep == 1 & REPLACED == 0), 1, 0),
           fn = ifelse((model_pred_rep == 0 & REPLACED == 1), 1, 0))
  for_consequences = test %>%
    filter(DATA_ELEMENT == data_element)
  tp = for_consequences %>%
    filter(tp == 1)
  tp_sum = sum(tp$DUPE_ANSWER)
  print(paste('True Positve Aggreagted', data_element,':', tp_sum))
  tn = for_consequences %>%
    filter(tn == 1)
  tn_sum = sum(tn$OG_ANSWER)
  print(paste('True Negative Aggreagted', data_element,':',tn_sum))
  fp = for_consequences %>%
    filter(fp == 1)
  fp_sum = sum(fp$DUPE_ANSWER)
  print(paste('False Positive Aggregated', data_element,':', fp_sum))
  fn = for_consequences %>%
    filter(fn == 1)
  fn_sum = sum(fn$OG_ANSWER)
  print(paste('False Negatives Aggregated', data_element,':', fn_sum))
}




# would like to find the AUC for individual trade areas
# intended for one model that includes all trades
auc_by_trade <- function(test, model, trade_area) {
  pred_test = predict(model, test, type = "response")
  pred_by_trade = prediction(subset(pred_test, test$TRADE_ASSIGNMENT == trade_area),
                             subset(test$REPLACED, test$TRADE_ASSIGNMENT == trade_area))
  perf = performance(pred_by_trade, "acc")
  max_ind = which.max(slot(perf, "y.values")[[1]])
  acc = slot(perf, "y.values")[[1]][max_ind]
  cutoff = slot(perf, "x.values")[[1]][max_ind]
  print(c(accuracy = acc, cutoff = cutoff))
  roc = performance(pred_by_trade,"tpr","fpr")
  auc = performance(pred_by_trade, measure = "auc")
  print(paste('AUC for trade area', trade_area,':', auc@y.values))
}



# a function for identifying accuracy metrics using the all trades model with the specific threshold that works best for a single trade
# intended to be used by largest and smallest trade and with the threshold found using auc_by_trade function created above
for_best_worst_trade_dollar_confusion_matrix <- function(test, threshold, pred_test, data_element, trade_area){
  test = test %>%
    mutate(pred = pred_test,
           model_pred_rep = ifelse(pred >= threshold, 1,0),
           tp = ifelse((model_pred_rep == 1 & REPLACED == 1), 1, 0),
           tn = ifelse((model_pred_rep == 0 & REPLACED == 0), 1, 0),
           fp = ifelse((model_pred_rep == 1 & REPLACED == 0), 1, 0),
           fn = ifelse((model_pred_rep == 0 & REPLACED == 1), 1, 0))
  test = test %>%
    filter(TRADE_ASSIGNMENT == trade_area)
  for_consequences = test %>%
    filter(DATA_ELEMENT == data_element)
  tp = for_consequences %>%
    filter(tp == 1)
  tp_sum = sum(tp$DUPE_ANSWER)
  print(paste('True Positve Aggreagted', data_element,':', tp_sum))
  tn = for_consequences %>%
    filter(tn == 1)
  tn_sum = sum(tn$OG_ANSWER)
  print(paste('True Negative Aggreagted', data_element,':',tn_sum))
  fp = for_consequences %>%
    filter(fp == 1)
  fp_sum = sum(fp$DUPE_ANSWER)
  print(paste('False Positive Aggregated', data_element,':', fp_sum))
  fn = for_consequences %>%
    filter(fn == 1)
  fn_sum = sum(fn$OG_ANSWER)
  print(paste('False Negatives Aggregated', data_element,':', fn_sum))
}


#create a function for making trade specific regularized models
# feed it a sample of just a single trade
trade_specific_regularized_model_creation <- function(trade_area_sample){
  
  # split sample into training/test data
  trade = (trade_area_sample$TRADE_ASSIGNMENT)[1]
  
  set.seed(4)
  sample1 <- sample.split(trade_area_sample$REPLACED, SplitRatio = 0.8)
  train  <- subset(trade_area_sample, sample1 == TRUE)
  test   <- subset(trade_area_sample, sample1 == FALSE)
  print(paste('Percent of sample that became training data: ',
              nrow(train)/nrow(trade_area_sample),
              'Percent of sample that became test data: ',
              nrow(test)/nrow(trade_area_sample)))
  
  # normalize training data
  train <- train %>% 
    mutate(HOURS_BETWEEN = scale(HOURS_BETWEEN),
           INPUT_DIFFERENCE = scale(INPUT_DIFFERENCE),
           BLANK_0_INTERACTION = scale(BLANK_0_INTERACTION),
           DUPE_HOURS_PAST_DUE_DATE = scale(DUPE_HOURS_PAST_DUE_DATE),
           DUPE_HOURS_PAST_1ST = scale(DUPE_HOURS_PAST_1ST),
           DUPE_HOURS_PAST_2ND = scale(DUPE_HOURS_PAST_2ND),
           DUPE_HOURS_PAST_MAIL_FOLLOW_UP = scale(DUPE_HOURS_PAST_MAIL_FOLLOW_UP),
           PROD_COUNT_DIF = scale(PROD_COUNT_DIF),DE_COUNT_DIF = scale(DE_COUNT_DIF))
  
  # create a design matrix for the training data to be able to feed into glmnet
    # categorical variables
  REPLACED_train = train$REPLACED
  SURVUNIT_TYP_train = as.factor(train$SURVUNIT_TYP)
  OG_BLANK_DUMMY_train = as.factor(train$OG_BLANK_DUMMY)
    # continous variables
  OG_BLANK_INPUT_INTERACTION_train = (train$BLANK_0_INTERACTION)
  HOURS_BETWEEN_train = train$HOURS_BETWEEN
  INPUT_DIFFERENCE_train = train$INPUT_DIFFERENCE
  DUPE_HOURS_PAST_DUE_DATE_train = train$DUPE_HOURS_PAST_DUE_DATE
  DUPE_HOURS_PAST_1ST_train = train$DUPE_HOURS_PAST_1ST
  DUPE_HOURS_PAST_2ND_train = train$DUPE_HOURS_PAST_2ND
  DUPE_HOURS_PAST_MAIL_FOLLOW_UP_train = train$DUPE_HOURS_PAST_MAIL_FOLLOW_UP
  PROD_COUNT_DIF_train = train$PROD_COUNT_DIF
  DE_COUNT_DIF_train = train$DE_COUNT_DIF
  
  # actual design matrix creation
  x.factors_train = model.matrix(REPLACED_train ~ SURVUNIT_TYP_train + 
                                   OG_BLANK_DUMMY_train )[,-1]
  
  x_train = as.matrix(data.frame(HOURS_BETWEEN_train,INPUT_DIFFERENCE_train,
                                 OG_BLANK_INPUT_INTERACTION_train,
                                 DUPE_HOURS_PAST_1ST_train, DUPE_HOURS_PAST_2ND_train,
                                 DUPE_HOURS_PAST_DUE_DATE_train,
                                 DUPE_HOURS_PAST_MAIL_FOLLOW_UP_train,
                                 PROD_COUNT_DIF_train, DE_COUNT_DIF_train,
                                 x.factors_train))
  
  
  # create a design matrix for the test data to be able to feed into glmnet
  # categorical variables
  REPLACED = test$REPLACED
  SURVUNIT_TYP = as.factor(test$SURVUNIT_TYP)
  OG_BLANK_DUMMY = as.factor(test$OG_BLANK_DUMMY)
  # continous variables
  OG_BLANK_INPUT_INTERACTION = (test$BLANK_0_INTERACTION)
  HOURS_BETWEEN = test$HOURS_BETWEEN
  INPUT_DIFFERENCE = test$INPUT_DIFFERENCE
  DUPE_HOURS_PAST_DUE_DATE = test$DUPE_HOURS_PAST_DUE_DATE
  DUPE_HOURS_PAST_1ST = test$DUPE_HOURS_PAST_1ST
  DUPE_HOURS_PAST_2ND = test$DUPE_HOURS_PAST_2ND
  DUPE_HOURS_PAST_MAIL_FOLLOW_UP = test$DUPE_HOURS_PAST_MAIL_FOLLOW_UP
  PROD_COUNT_DIF = test$PROD_COUNT_DIF
  DE_COUNT_DIF = test$DE_COUNT_DIF
  
  
  x.factors_test = model.matrix(REPLACED ~ SURVUNIT_TYP  + 
                                   OG_BLANK_DUMMY )[,-1]
  x_test = as.matrix(data.frame(HOURS_BETWEEN,INPUT_DIFFERENCE,DUPE_HOURS_PAST_1ST,
                                OG_BLANK_INPUT_INTERACTION,
                                DUPE_HOURS_PAST_2ND, DUPE_HOURS_PAST_DUE_DATE,
                                DUPE_HOURS_PAST_MAIL_FOLLOW_UP, PROD_COUNT_DIF, 
                                DE_COUNT_DIF, x.factors_test))
  
  
  # add weights to trianing data
  proportion_1 = (length((train$REPLACED)[train$REPLACED == 1]))/(length(train$REPLACED))
  train$weights = ifelse(train$REPLACED == 0, proportion_1, (1 - proportion_1))

  # create a model using training data
  fit <- glmnet(x_train, y=(REPLACED_train), weights = train$weights, family="binomial")
  
  # plot regularization path -labels only the top 30 largest coefficients
    # - i have much less so will label all
  print('regularization path plotted in plots view but overwritten by cross-validation plot - scroll through plots')
  print(paste(plot_glmnet(fit, label=30)))
  
  # cross validation to find the best lambda value
  cvfit = cv.glmnet(x_train, y = (REPLACED_train), weights = train$weights,
                    family = "binomial", type = "class")
  print('cross-validation path plotted in plots view but overwritten by ROC/AUC plot - scroll through plots')
  print(paste(plot(cvfit)))
  
  # predictions on the test set
  predictions <- predict(fit, newx = x_test, type = 'response', s= cvfit$lambda.1se)
  predictions_cv = predict(cvfit, newx = x_test, type = 'response', s = cvfit$lambda.1se)
  print(predict(cvfit,type="coef", s = cvfit$lambda.1se))

  # assessing fit
  accuracy_metrics = assess.glmnet(fit, newx = x_test, newy = REPLACED, family = "binomial", s= cvfit$lambda.1se)
  print(accuracy_metrics)
  
  
  # plotting ROC curve and finding AUC
  # use this plot to better predict where cutoff would be best 
  pred = prediction(predictions, REPLACED)
  perf = performance(pred, "acc")
  print(paste(plot(perf)))
  
  # tells us what most accurate cutoff point would be  
  max_ind = which.max(slot(perf, "y.values")[[1]] )
  acc = slot(perf, "y.values")[[1]][max_ind]
  cutoff = slot(perf, "x.values")[[1]][max_ind]
  print(c(accuracy= acc, cutoff = cutoff))
  
  # creates ROC plot and prints out AUC value
  roc = performance(pred,"tpr","fpr")
  plot(roc, colorize = F, lwd = 2)
  abline(a = 0, b = 1)
  auc = performance(pred, measure = "auc")
  print(auc@y.values) 
  
  # is model good enough
  good_enough = auc@y.values>proportion_1
  ifelse(good_enough == T, print(paste("AUC value is larger than the baseline of replaced/not-replaced cases for trade area" , 
                                       trade, "so, model is acceptable.")), 
               print(paste("AUC value is smaller than the baseline of replaced/not-replaced cases for trade area", 
               trade, "so, model is not acceptable.")))
}



