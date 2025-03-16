library(dplyr)
library(openxlsx)
library(readxl)
library(ggplot2)
library(zoo)
setwd("C:/Users/lawre/OneDrive/Documents/Joebucsfan_pipeline")

#Create a function to save each individual graph
save_to_jpeg <- function(file_name) {
  ggsave(
    filename = paste0("Analysis/", file_name, ".jpeg"), 
    plot = last_plot(), 
    width = 8, 
    height = 6, 
    dpi = 300,
    device = "jpeg"
  )
}


#Read in Comments dataset
comments_data <- read_excel(paste0(getwd(), "/Snowflake_data/joebucs_comments.xlsx")) %>%
  mutate(COMMENT_POST_TIME = as.POSIXct(COMMENT_POST_TIME, format = "%Y-%m-%d %H:%M:%S"),
         COMMENT_WORD_COUNT = as.integer(COMMENT_WORD_COUNT)) 
#Set max/min dates for subtitles in graphs
min_comment_date<-format(min(comments_data$POST_TIME), "%m/%d/%Y")
max_comment_date<-format(max(comments_data$POST_TIME), "%m/%d/%Y")

post_count <- comments_data %>%
  distinct(TITLE) %>%
  nrow()

#Find the top 25 commenters by response rate
top_25 <- comments_data %>%
  distinct(USERNAME, TITLE) %>%
  group_by(USERNAME) %>%
  summarise(comment_count = n()) %>%
  ungroup() %>%
  mutate(comment_perc = comment_count / post_count) %>%
  arrange(desc(comment_perc)) %>%
  top_n(25, comment_perc)
#Find the average time to comment for each commenter
avg_time_to_comment <- comments_data %>% select(USERNAME, TITLE, HRS_TO_RESPONSE) %>%
  group_by(USERNAME, TITLE) %>%
  filter(HRS_TO_RESPONSE == min(HRS_TO_RESPONSE)) %>% ungroup() %>%
  group_by(USERNAME) %>%
  summarise(avg_time_to_comment = mean(HRS_TO_RESPONSE))
#Find the average word count for each commenter
avg_comment_word_count <- comments_data %>% select(USERNAME, TITLE, COMMENT_WORD_COUNT) %>%
  group_by(USERNAME) %>%
  summarise(avg_word_count = mean(COMMENT_WORD_COUNT))

#Create single data frame with multiple commenter level summaries
combined <- top_25 %>% left_join(avg_time_to_comment) %>% left_join(avg_comment_word_count)

#Commenter Response rate for top 25 commenters
ggplot(top_25, aes(x = reorder(USERNAME, -comment_perc), y = comment_perc)) +
  geom_bar(stat = "identity", fill = "steelblue") +  # Same fill color for every bar
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(
    title = "Commenter Response Rate (% of Articles Commented on)",
    subtitle = paste0("Based on data from ", min_comment_date, " to ", max_comment_date),
    x = "Username",
    y = "Response Rate"
  ) +
  scale_x_discrete(drop = FALSE) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 6),
    plot.title = element_text(hjust = 0.5),
    plot.subtitle = element_text(hjust = 0.5))
save_to_jpeg(file_name = "commenter_response_rate")

# Read in the article level dataset
post_data <- read_excel(paste0(getwd(), "/Snowflake_data/joebucs_extract.xlsx")) %>%
  mutate(NUMBER_OF_COMMENTS = as.integer(NUMBER_OF_COMMENTS),
                                    POST_DATE = as.Date(POST_TIME)) %>%
  filter(NUMBER_OF_COMMENTS > 0)
#Calculate daily summary metrics
combined_data <- post_data %>%
  group_by(POST_DATE) %>%
  summarise(
    post_count = n(),
    comment_count = sum(NUMBER_OF_COMMENTS),
    avg_article_sentiment = mean(ARTICLE_SENTIMENT_SCORE),
    avg_commenter_sentiment = mean(RESPONSE_SENTIMENT_SCORE),
    comments_per_article = comment_count / post_count
  ) %>% arrange(POST_DATE) %>%  # Ensure data is sorted by date
  mutate(
    avg_article_sentiment_7day = rollmean(avg_article_sentiment, k = 7, fill = NA, align = "right"),
    avg_commenter_sentiment_7day = rollmean(avg_commenter_sentiment, k = 7, fill = NA, align = "right")
  )

### Number of posts per day ###
ggplot(combined_data, aes(x = POST_DATE, y = post_count)) +
  geom_line() +
  geom_point() +
  labs(title = "# of Articles By Date",
       subtitle = paste0("Based on data from ", min_comment_date, " to ", max_comment_date),
       x = "Article Date",
       y = "Number of Articles") +
  scale_y_continuous(breaks = seq(min(combined_data$post_count), max(combined_data$post_count), by = 2))+
  theme_minimal()+
  theme(plot.title = element_text(hjust = 0.5),
        plot.subtitle = element_text(hjust = 0.5))

save_to_jpeg(file_name = "number_of_posts_per_day")

### Number of comments per post ###
ggplot(combined_data, aes(x = POST_DATE, y = comments_per_article)) +
  geom_line() +
  geom_point() +
  labs(title = "Comments per Article by Date",
       subtitle = paste0("Based on data from ", min_comment_date, " to ", max_comment_date),
       x = "Article Date",
       y = "Number of Comments Per Article") +
  #Adds 0% pad below lowest point; adds 10% pad above highest point; using 'mult' argument because it expands based 
  #on % of highest/lowest, could use 'add' argument which would adjust by a fixed amount
  scale_y_continuous(expand = expansion(mult = c(0, 0.1))) +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5),
        plot.subtitle = element_text(hjust = 0.5))

save_to_jpeg(file_name = "comments_per_post_per_day")

# Graph with daily sentiment and 7-day rolling average
ggplot(combined_data, aes(x = POST_DATE)) +
  geom_line(aes(y = avg_article_sentiment, color = "Daily Article Sentiment"), linewidth = 0.5) +
  geom_line(aes(y = avg_commenter_sentiment, color = "Daily Commenter Sentiment"), linewidth = 0.5) +
  geom_point(aes(y = avg_article_sentiment, color = "Daily Article Sentiment"), size = 2) +
  geom_point(aes(y = avg_commenter_sentiment, color = "Daily Commenter Sentiment"), size = 2) +
  
  # Add 7-day rolling average lines
  geom_line(aes(y = avg_article_sentiment_7day, color = "Rolling 7-Day Avg Article Sentiment"), linewidth = 1) +
  geom_line(aes(y = avg_commenter_sentiment_7day, color = "Rolling 7-Day Avg Commenter Sentiment"), linewidth = 1) +
  
  scale_color_manual(
    name = NULL,
    values = c(
      "Daily Article Sentiment" = "blue",
      "Daily Commenter Sentiment" = "red",
      "Rolling 7-Day Avg Article Sentiment" = "darkblue",
      "Rolling 7-Day Avg Commenter Sentiment" = "darkred"
    )
  ) +
  labs(
    title = "Average Sentiment Scores Over Time",
    subtitle = paste0("Based on data from ", min(combined_data$POST_DATE), " to ", max(combined_data$POST_DATE)),
    x = "Date",
    y = "Average Sentiment Score (0-10)"
  ) +
  scale_y_continuous(
    limits = c(0, 10),
    breaks = seq(0, 10, 1)
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(hjust = 0.5, size = 12),
    plot.subtitle = element_text(hjust = 0.5, size = 8),
    axis.text = element_text(size = 8),
    axis.title = element_text(size = 10),
    legend.title = element_blank(),
    legend.text = element_text(size = 6) # Adjust legend text size
  )

save_to_jpeg(file_name = "daily_avg_sentiment_scores")

