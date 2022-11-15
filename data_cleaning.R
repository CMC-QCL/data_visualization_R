## This data cleaning process is not perfect, 
## it was performed to obtain a dataset used for illustrative purposes only.
## Furthermore, there's missing data that was imputed to get the full dataset
## and a sample slice was taken afterwards.

library(tidyverse)
library(missForest)

# Define NOT IN
'%ni%' <- Negate('%in%')

# Read all files (add platform column)
netflix <- read_csv('data/netflix_titles.csv') %>%
  mutate(platform = 'netflix')

disney <- read_csv('data/disney_plus_titles.csv') %>%
  mutate(platform = 'disney')

hulu <- read_csv('data/hulu_titles.csv') %>%
  mutate(platform = 'hulu')

prime <- read_csv('data/amazon_prime_titles.csv') %>%
  mutate(platform = 'prime')

imdb.titles <- read_tsv('data/imdb/title.basics.tsv')
imdb.ratings <- read_tsv('data/imdb/title.ratings.tsv')

movies.metadata <- read_csv('data/movies_metadata.csv')

# Merge all platforms
merged.platforms <- bind_rows(netflix, disney, hulu, prime)

# Fill director, casting and country information 
# using information from all platforms
platforms <- merged.platforms %>% 
  group_by(title) %>%
  fill(director, cast, country, .direction = "downup") %>%
  ungroup()

## Join IMDB info and stack all titles
# Only keep the original title column
original.titles <- imdb.titles %>% 
  select(!primaryTitle) %>% 
  rename(title = originalTitle)

# Only keep the primary title column
primary.titles <- imdb.titles %>% 
  select(!originalTitle) %>% 
  rename(title = primaryTitle)

# Stack both titles (original and primary) and join with IMDB ratings
imdb <- original.titles %>%
  bind_rows(primary.titles) %>%
  inner_join(imdb.ratings, by = "tconst") %>%
  distinct(titleType, title, startYear, .keep_all = TRUE)  # Removes rows with duplicated titles

# Clean the IMDB data
imdb_clean <- imdb %>%
  filter(startYear != '\\N') %>%  # Remove rows without startYear
  mutate(across(startYear, as.integer)) %>%  # Change column type to integer
  mutate(recoded_titleType = case_when(
    str_detect(titleType, regex("Movie", ignore_case = TRUE)) ~ "Movie",
    str_detect(titleType, regex("Short", ignore_case = TRUE)) ~ "Movie",
    str_detect(titleType, regex("Series", ignore_case = TRUE)) ~ "TV Show"
  ))

# Join the movies metadata to get the budget and revenue
imdb_extra <- imdb_clean %>%
  left_join(distinct(movies.metadata, imdb_id, budget, revenue), 
            by = c("tconst" = "imdb_id"), 
            suffix = c("", ".y")) %>%
  select(c(colnames(imdb_clean), "budget", "revenue"))

# Select only movies and shows with IMDB data
merged_dataset <- imdb_extra %>%
  inner_join(platforms, by = c("title", 
                               "startYear" = "release_year", 
                               "recoded_titleType" = "type")) %>%
  select(c(colnames(platforms)[colnames(platforms) %ni% c("release_year", "type")], 
           colnames(imdb_extra))) %>%  # Reorder the columns
  select(!c("show_id", "description", "rating", "tconst", 
            "endYear", "titleType", "cast", "runtimeMinutes", "isAdult",
            "listed_in", "director", "duration")) %>%
  distinct(title, platform, .keep_all = TRUE)  # Make sure we don't keep duplicated titles from the same platform

# Add columns for each classification in genres
genres_columns <- merged_dataset %>%
  pull(genres) %>%
  str_c(collapse = ',') %>%
  str_split(',') %>%
  flatten_chr() %>%
  str_sort() %>%
  unique() %>%
  str_replace("-", ".")

new_columns_dataset <- merged_dataset %>% 
  add_column(dummy = 0) %>%  # Add dummy column
  separate(dummy, genres_columns[genres_columns != "\\N"])  # "separate" to get the new columns added

# Create the presence absence matrix in the data frame
full_dataset <- new_columns_dataset %>%
  mutate(Action = if_else(str_detect(genres, "Action"), 1, 0),
         Adult = if_else(str_detect(genres, "Adult"), 1, 0),
         Adventure = if_else(str_detect(genres, "Adventure"), 1, 0),
         Animation = if_else(str_detect(genres, "Animation"), 1, 0),
         Biography = if_else(str_detect(genres, "Biography"), 1, 0),
         Comedy = if_else(str_detect(genres, "Comedy"), 1, 0),
         Crime = if_else(str_detect(genres, "Crime"), 1, 0),
         Documentary = if_else(str_detect(genres, "Documentary"), 1, 0),
         Drama = if_else(str_detect(genres, "Drama"), 1, 0),
         Family = if_else(str_detect(genres, "Family"), 1, 0),
         Film.Noir = if_else(str_detect(genres, "Film-Noir"), 1, 0),
         Game.Show = if_else(str_detect(genres, "Game-Show"), 1, 0),
         History = if_else(str_detect(genres, "History"), 1, 0),
         Horror = if_else(str_detect(genres, "Horror"), 1, 0),
         Music = if_else(str_detect(genres, "Music"), 1, 0),
         Musical = if_else(str_detect(genres, "Musical"), 1, 0),
         Mystery = if_else(str_detect(genres, "Mystery"), 1, 0),
         News = if_else(str_detect(genres, "News"), 1, 0),
         Reality.TV = if_else(str_detect(genres, "Reality-TV"), 1, 0),
         Romance = if_else(str_detect(genres, "Romance"), 1, 0),
         Sci.Fi = if_else(str_detect(genres, "Sci-Fi"), 1, 0),
         Short = if_else(str_detect(genres, "Short"), 1, 0),
         Sport = if_else(str_detect(genres, "Sport"), 1, 0),
         Talk.Show = if_else(str_detect(genres, "Talk-Show"), 1, 0),
         Thriller = if_else(str_detect(genres, "Thriller"), 1, 0),
         War = if_else(str_detect(genres, "War"), 1, 0),
         Western = if_else(str_detect(genres, "Western"), 1, 0))

# Impute budget and revenue data
reduced_imputed_dataset <- full_dataset %>%
  filter(recoded_titleType == "Movie") %>%
  select(all_of(c("startYear", "averageRating", 
                  "numVotes", "budget", "revenue", 
                  genres_columns[genres_columns != "\\N"]))) %>%
  mutate(budget = na_if(budget, 0), revenue = na_if(revenue, 0)) %>%
  as.data.frame() %>%
  missForest(maxiter = 100, ntree = 500)

# Join with the rest of the data
imputed_dataset <- full_dataset %>%
  select(!c("budget", "revenue")) %>%  #Make space for the updated columns
  left_join(reduced_imputed_dataset$ximp)

# Take just 10% of the rows in each platform
final_dataset <- imputed_dataset %>%
  group_by(platform) %>%
  slice_sample(prop = 0.1) %>%
  ungroup() %>%
  rename(type = recoded_titleType) %>%
  select(!c("genres", "Adventure", "Adult", "Animation", "Biography", "Crime", "Documentary", "Family",
            "Fantasy", "Film.Noir", "Game.Show", "History", "Music", "Musical", "Mystery", "News", 
            "Reality.TV", "Short", "Sport", "Talk.Show", "War", "Western")) %>% 
  mutate(sum = rowSums(across(c(Action, Comedy, Drama, Horror, 
                                Romance, Sci.Fi, Thriller)))) %>%
  filter(sum != 0) %>%
  select(!sum)

# Last cleaning
final_dataset$country <- final_dataset$country %>% 
  str_split(',') %>% 
  map(1) %>% 
  as.character()

# Save the dataset
write_csv(final_dataset, "data/streaming_services.csv")
