DATADIR = '/Users/rodrigo/Documents/Big-Data/homework/homework2'
zip_file_path = '/Users/rodrigo/Documents/Big-Data/homework/awards/awards.zip'

library(magrittr)
library(NLP)
library(tm)
library(fasttime)
library(data.table)
library(stringr)
library(parallel)

process_one = function(name_agency_csv, n)
{
  df = fread(name_agency_csv, select = c('total_obligation', 'description'))  
  
  #taking care of NA and negative numbers
  df = na.omit(df)
  df = df[df$total_obligation > 0 ,]
  
  #text preprocessing
  df$description = df$description %>%
    as.character() %>%
    removePunctuation() %>%
    removeNumbers() %>%
    tolower() %>%
    stripWhitespace() %>%
    removeWords(stopwords('en')) %>%
    stemDocument()
  
  #splitting the sentences into words and calculating the number of words per row
  words = sapply(df$description, str_split, pattern = ' ', USE.NAMES = FALSE)
  number_words = sapply(df$description, function(x) str_count(x, pattern = ' ') + 1,
                        USE.NAMES = FALSE)
  
  weight = df$total_obligation/number_words
  weight = rep(weight, number_words)
  
  #combining everything
  df_all_words = data.frame(words = unlist(words), weight = weight)
  df_final = aggregate(weight ~ words, df_all_words, sum)
  #only words with more than 3 characters
  df_final = df_final[(nchar(as.character(df_final$words)) > 3),]
  
  #normalizing the weights and sorting by weight
  df_final$weight = df_final$weight/sum(df_final$weight)
  df_most_weighted = (df_final[order(- df_final$weight) ,])[1:n ,]
  
  return(df_most_weighted)
}

#correct files
zip_file_path = '~/Documents/Big-Data/homework/awards/awards.zip'
files = unzip(zip_file_path, list = TRUE)
files_correct_size = files[files$Length > 1048576 & files$Length < 50000000 ,]$Name

#computing top 25 words for all 91 agencies
total = lapply(files_correct_size, process_one)


##parallelization##

#create a cluster and share the information to the workers
cls = makeCluster(8L, type = 'PSOCK')
clusterCall(cls, source, 'preprocess.R')

#try differents parallel::functions
total_parallel_pL = parLapply(cls, files_correct_size, process_one)
total_parallel_pLB = parLapplyLB(cls, files_correct_size, process_one)
total_parallel_cL = clusterApply(cls, files_correct_size, process_one)
total_parallel_cLB = clusterApplyLB(cls, files_correct_size, process_one)

stopCluster(cls)