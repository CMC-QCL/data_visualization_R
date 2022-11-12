library(tidyverse)

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

# Select only movies and shows with IMDB data
full_dataset <- imdb_clean %>%
  inner_join(platforms, by = c("title", 
                               "startYear" = "release_year", 
                               "recoded_titleType" = "type")) %>%
  select(c(colnames(platforms)[colnames(platforms) %ni% c("release_year", "type")], 
           colnames(imdb))) %>%  # Reorder the columns
  select(!c("show_id", "description", "rating", "tconst", 
            "endYear", "titleType", "cast", "date_added")) %>%
  distinct(title, platform, .keep_all = TRUE)  # Make sure we don't keep duplicated titles from the same platform

# Take just 10% of the rows in each platform
final_dataset <- full_dataset %>%
  group_by(platform) %>%
  slice_sample(prop = 0.1)

# Save the dataset
write_csv(final_dataset, "data/streaming_services.csv")