from bs4 import BeautifulSoup
import requests
import re
import json
import csv
import os
import openai
from datetime import datetime, timedelta
from openai import OpenAI
import copy

# Constants and Setup
lag_days = 2
page_numbers = [1,2,3]
api_key = os.getenv("OPENAI_API_KEY") 
client = OpenAI(api_key=api_key)
base_url = 'http://joebucsfan.com'
# Utility Functions
def date_conversion(date):
    """Converts date string from blog format to a datetime object."""
    date_string = ' '.join(date)
    for suffix in ['st,', 'nd,', 'rd,', 'th,']:
        date_string = date_string.replace(suffix, '')
    try:
        return datetime.strptime(date_string, "%B %d %Y at %I:%M %p")
    except ValueError:
        return None

def fetch_and_parse(url):
    """Fetches page content and returns a BeautifulSoup object."""
    response = requests.get(url)
    return BeautifulSoup(response.text, 'lxml')

def analyze_text(text, title, connection):
    """Analyzes article text using OpenAI for sentiment and summary."""
    completion = connection.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": f"Provide a sentiment score with 0 being maximally pessimistic and 10 being maximally optimistic for an article titled {title} with the following content: {text[0:16000]}. Only provide the numeric score with no commentary. Give the name of the subject of the article. Give a summary of the article in 10 words or less. Provide the response in the form: 'sentiment score, subject of the article, summary of the article'."}
        ]
    )
    return completion.choices[0].message.content.split(",")

def analyze_comments(comments_text, connection):
    """Analyzes comments using OpenAI for sentiment score and summary."""
    comment_analysis = connection.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": f"Provide a sentiment score with 0 being maximally pessimistic and 10 being maximally optimistic for the following comments in total (i.e., don't provide a score for each comment): {comments_text[0:16000]}. Only provide the numeric score with no commentary. Also, provide a summary of the responses in 15 words or less. Structure the response in the following way with zero exceptions: 'sentiment score; summary' (e.g., 7; The team is good)."}
        ]
    )
    return comment_analysis.choices[0].message.content.split(";")

def scrape_articles(page_numbers, url_input, client):

    """Scrapes articles and comments from the specified pages."""
    dataset = {}
    art_count = 0
    target_date = datetime.now().date() - timedelta(days=lag_days)
    
    for i in page_numbers:
        url = f'{url_input}/page/{i}/' if i != 1 else url_input
        soup = fetch_and_parse(url)
        articles = soup.find_all('div', class_="post")
    
        for article in articles:
            link = article.find('a').get('href')
            title = article.a.text.replace("\n", " ")
            article_soup = fetch_and_parse(link)
            post_time = article_soup.find('meta', property="article:published_time")['content'][:16]
            post_time = datetime.strptime(post_time, '%Y-%m-%dT%H:%M') -  timedelta(hours = 4)
    
            if post_time.date() != target_date:
                continue
    
            # Article text and analysis
            text = " ".join([p.text for p in article_soup.find_all(style='text-align: left;')]).replace("\n", " ")
            article_sentiment = analyze_text(text, title, connection = client)
    
            # Collect and analyze comments
            comments = {}
            comments_only = {}
            for idx, comment in enumerate(article_soup.find_all('li', id=re.compile(r'comment'))):
                poster = comment.find('cite').text
                comment_text = comment.find('p').text.replace("\n", " ")
                comment_time = comment.find('a').text.split(" ") #date_conversion(comment.find('a').text.split(" "))
                comments[f"commenter{idx}"] = {
                    "username": poster,
                    "post": comment_text,
                    "post_time": comment_time
                }
                # Store only the comment text in comments_only with the same key
                comments_only[f"commenter{idx}"] = comment_text
    
            # Prepare a single string of all comments for sentiment analysis
            comments_text = " ".join([f"{key}:{value}" for key, value in comments_only.items()])
            # Remove any character repeated 5 or more times in a row, truncating to 4 repeats
            comments_text = re.sub(r'(.)\1{4,}', r'\1\1\1\1', comments_text)
            if comments_text.strip():  # Only analyze if there are comments
                comments_sentiment = analyze_comments(comments_text, connection= client)
                comments_sentiment_score = comments_sentiment[0].strip()
                comments_summary = comments_sentiment[1].strip()
            else:
                comments_sentiment_score = None
                comments_summary = "No comments available."
    
            # Store article data
            article_dict = {
                "address": link,
                "title": title,
                "post": text,
                "post_time": post_time,
                "word_count": len(text.split()),
                "number_of_comments": len(comments),
                "article_sentiment_score": article_sentiment[0],
                "article_subject": article_sentiment[1].strip(),
                "article_summary": article_sentiment[2].strip(),
                "response_sentiment_score": comments_sentiment_score,
                "response_summary": comments_summary,
                "responses": comments
            }
    
            dataset[f'article_{art_count}'] = article_dict
            art_count += 1
    return dataset
    

def save_to_csv(data, filename, dataset_type="article"):
    """
    Saves dataset to a CSV file. It can handle either 'article' or 'comment' datasets.

    Parameters:
    - dataset: The main dataset to be saved (for articles).
    - filename: The name of the file to save (without extension).
    - dataset_type: "article" or "comment", specifying which type of CSV to create.
    - comments_data: Optional, dictionary of comments if saving comments CSV.
    """
    file_path = os.path.join("/opt/airflow/output", "Extracts", f"{filename}.csv")
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    data_copy = copy.deepcopy(data)
    if dataset_type == "article":
        fieldnames = [
        'address', 'title', 'post', 'post_time', 'word_count', 'number_of_comments',
        'article_sentiment_score', 'article_subject', 'article_summary','response_sentiment_score',
        'response_summary', 'responses'
        ]
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter="|")
        
            for article in data_copy.values():
                article['responses'] = json.dumps(article['responses'])
                article_data = {k: v for k, v in article.items() if k in fieldnames}
                writer.writerow(article_data)

    elif dataset_type == "comment": 
        fieldnames = [
        'username', 'comment_post_time','article_title', 'article_post_time', 
        'comment_word_count', 'hrs_to_response'
        ]
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter="|")
            for key, article in data_copy.items():

                for key, responses in article['responses'].items():
                    if date_conversion(responses['post_time']) == None:
                        hrs_to_response = 0
                    else: 
                        hrs_to_response = (date_conversion(responses['post_time']) - article['post_time']).total_seconds() / (60*60)
                    writer.writerow({
                        'username': responses['username'],
                        'comment_post_time': date_conversion(responses['post_time']),
                        'article_title': article['title'],
                        'article_post_time': article['post_time'],
                        'comment_word_count': len(responses['post'].encode('utf-8').decode('utf-8', 'ignore').split()),
                        'hrs_to_response': hrs_to_response 
                    })


