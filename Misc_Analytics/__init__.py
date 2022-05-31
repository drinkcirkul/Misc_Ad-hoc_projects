
import datetime
import logging

import azure.functions as func

#basic package needed
from time import sleep
import random
import numpy as np
import pandas as pd

from facebook_scraper import get_posts
import pandas as pd
import sqlalchemy
import psycopg2
from sqlalchemy import create_engine

from datetime import date, datetime, timedelta

import requests 
import json
import datetime as dt
#NLP related package
import spacy
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize 
import itertools
import string
from nltk.sentiment import SentimentIntensityAnalyzer
#dowload related library
#import en_core_web_sm
#nlp = en_core_web_sm.load()
nltk.download('vader_lexicon')
vader = SentimentIntensityAnalyzer()
nlp = spacy.load("en_core_web_sm")
nltk.download('stopwords')
nltk.download('punkt')

#facebook scraper function
#facebook scraper function
def facebook_group_scraper(group_name,date_range,cookie_json):
    #empty list to store info
    text_list=[]
    #counter for potential early stop action
    n=0
    #scrape data from facebook group with specific cookie
    master=get_posts(group=group_name,options={'comments': True},cookies=cookie_json,timeout=15)
    print('1')
    #show the group scraped 
    print(group_name)
    #loop to interate through the extracted data
    for post in master:
        #get today date and post date
        today = datetime.combine(date.today(), datetime.min.time())
        post_date=post['time']
        #if find the post data is null skip this iteration 
        if post_date==None:
            continue       
        #add random sleep time to be lower the risk of banned
        sleep(random.uniform(1, 3))
        # check whether the post is within the date range
        if (today-post_date).days<=date_range:
            #tmp list to pack each iteration
            tmp_list=[]
            #get all information needed into tmp list
            tmp_list.append(post['text'])
            tmp_list.append(post['time'])
            tmp_list.append(post['username'])
            tmp_list.append(post['post_id'])
            tmp_list.append('0')
            tmp_list.append(post['likes'])
            tmp_list.append(post['comments'])
            tmp_list.append(post['shares'])
            tmp_list.append('facebook_group')
            
            text_list.append(tmp_list)
            #key='comments_full'
            #get the comments related to the post
            if post['comments_full']!=None:
                for comment in post['comments_full']:
                    #tmp list to pack each iteration
                    tmp_list=[]
                    #get all information needed into tmp list
                    tmp_list.append(comment['comment_text'])
                    tmp_list.append(comment['comment_time'])
                    tmp_list.append(comment['commenter_name'])
                    tmp_list.append(post['post_id'])
                    tmp_list.append('1')
                    tmp_list.append(None)
                    tmp_list.append(None)
                    tmp_list.append(None)
                    tmp_list.append('facebook_group')

                    text_list.append(tmp_list)
                    #indicate the bottom of the each interation
                    print('bottom')
                    #get the counter plus 1
#                     n=n+1
#         #early stop by counter
#         if n>0:
#             break
    #convert the list into numpy
    array=np.array(text_list)
    #convert the numpy array to dataframe
    array=pd.DataFrame(array,columns=['comment', 'time', 'name','post_id','if_comment','likes','comment_num','share_num','platform'])
    #add all varder sentiment score based on the comment text
    array['positive_sentiment']=array['comment'].apply(lambda x: vader.polarity_scores(x)['pos'])
    array['neutral_sentiment']=array['comment'].apply(lambda x: vader.polarity_scores(x)['neu'])
    array['negative_sentiment']=array['comment'].apply(lambda x: vader.polarity_scores(x)['neg'])
    array['compound_sentiment']=array['comment'].apply(lambda x: vader.polarity_scores(x)['compound'])
    #indicate the finish state of the function
    print('done')
    return array

def insta_scrapper(tag_name,date_range,insta_sessionid):
    #sessionID unique for different instagrame ID
    SESSIONID =insta_sessionid
    print(SESSIONID)
    #header which specific the machine which applys to the request
    headers = {"user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Mobile Safari/537.36 Edg/87.0.664.57",
           "cookie": f"sessionid={SESSIONID};"}
    #dynamic linktake hastag name
    base_url = f'https://www.instagram.com/explore/tags/{tag_name}/?__a=1'
    print(base_url)
    #initial empty list
    text_list=[] 
    #initial empty max id which used to flip pages
    max_id=False
    #loop that scrape each page of the hastag
    for i in range(0, 20):
        print(i)
        #sleep to lower the risk of banned
        sleep(random.uniform(1, 3)) 
        #check whether exist a next_max_id
        if max_id:
            url = base_url + f"&next_max_id={max_id}"
        else:
            url = base_url
        #show which link we are scraping
        print(f"Requesting {url}")
        #extract the information
        response = requests.get(url,headers=headers)
        print(response)
        #convert the json to readable form
        response = json.loads(response.text)
        #flip to the next page or end
        try:
            max_id = response['data']['top']['next_max_id']
            print(f"New cursor is {max_id}")
        except KeyError:
            print("There's no next page!")
            break 
        #get the recent post information through json structured data 
        for i in response['data']['recent']['sections']:
            #get the inter list of content
            inter_list=i['layout_content']['medias']
            for j in inter_list:
                #get the today date and post date
                today = datetime.combine(date.today(), datetime.min.time())
                post_date=dt.datetime.fromtimestamp(int(j['media']['caption']['created_at']))
                #check whether the post is in date range
                if (today-post_date).days<=date_range:    
                    tmp_list=[]
                    tmp_list.append(j['media']['caption']['text'])
                    tmp_list.append(dt.datetime.fromtimestamp(int(j['media']['caption']['created_at'])).strftime('%Y-%m-%d %H:%M:%S'))
                    tmp_list.append(j['media']['user']['username'])
                    tmp_list.append(j['media']['id'])
                    tmp_list.append(0)
                    tmp_list.append(j['media']['like_count'])
                    tmp_list.append(j['media']['comment_count'])
                    tmp_list.append(0)
                    tmp_list.append('instagram')

                    text_list.append(tmp_list)
                    #get the comments from the post
                    for k in j['media']['comments']:
                        tmp_list=[]
                        tmp_list.append(k['text'])
                        tmp_list.append(dt.datetime.fromtimestamp(int(k['created_at'])).strftime('%Y-%m-%d %H:%M:%S'))
                        tmp_list.append(k['user']['username'])
                        tmp_list.append(j['media']['id'])
                        tmp_list.append(1)
                        tmp_list.append(None)
                        tmp_list.append(None)
                        tmp_list.append(None)
                        tmp_list.append('instagram')

                        text_list.append(tmp_list)


        #get the top post information through json structured data 
        for i in response['data']['top']['sections']:
            #get the inter list of content
            inter_list=i['layout_content']['medias']
            for j in inter_list:
                #get the today date and post date
                today = datetime.combine(date.today(), datetime.min.time())
                post_date=dt.datetime.fromtimestamp(j['media']['caption']['created_at'])
                a=j['media']['caption']['created_at']
                #print(post_date,j['media']['caption']['created_at'],type(post_date))
                if (today-post_date).days<=date_range:    
                    tmp_list=[]
                    tmp_list.append(j['media']['caption']['text'])
                    tmp_list.append(dt.datetime.fromtimestamp(int(j['media']['caption']['created_at'])).strftime('%Y-%m-%d %H:%M:%S'))
                    tmp_list.append(j['media']['user']['username'])
                    tmp_list.append(j['media']['id'])
                    tmp_list.append(0)
                    tmp_list.append(j['media']['like_count'])
                    tmp_list.append(j['media']['comment_count'])
                    tmp_list.append(0)
                    tmp_list.append('instagram')

                    text_list.append(tmp_list)
                    #get the comments from the post
                    for k in j['media']['comments']:
                        tmp_list=[]
                        tmp_list.append(k['text'])
                        tmp_list.append(dt.datetime.fromtimestamp(int(k['created_at'])).strftime('%Y-%m-%d %H:%M:%S'))
                        tmp_list.append(k['user']['username'])
                        tmp_list.append(j['media']['id'])
                        tmp_list.append(1)
                        tmp_list.append(None)
                        tmp_list.append(None)
                        tmp_list.append(None)
                        tmp_list.append('instagram')

                        text_list.append(tmp_list)

    #turn the list to numpy then numpy to dataframe
    array=np.array(text_list)
    array=pd.DataFrame(array,columns=['comment', 'time', 'name','post_id','if_comment','likes','comment_num','share_num','platform'])
    #get the varder sentiment score
    array['positive_sentiment']=array['comment'].apply(lambda x: vader.polarity_scores(x)['pos'])
    array['neutral_sentiment']=array['comment'].apply(lambda x: vader.polarity_scores(x)['neu'])
    array['negative_sentiment']=array['comment'].apply(lambda x: vader.polarity_scores(x)['neg'])
    array['compound_sentiment']=array['comment'].apply(lambda x: vader.polarity_scores(x)['compound'])
    #indicate the finish state of function
    print('done')
    
    return array
#uploader function that take dataframe and inject to specific datatable in panoply
def uploader(df,datatable):
    #credentials
    POSTGRES_ADDRESS = 'db.panoply.io'
    POSTGRES_PORT = '5439'
    POSTGRES_USERNAME = 'vikas+bumsbacapstone2021@drinkcirkul.com'
    POSTGRES_PASSWORD = '3US&cxaQ3'
    POSTGRES_DBNAME = 'cirkul_analytics'
    #data base form
    postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.
                    format(username=POSTGRES_USERNAME, 
                        password=POSTGRES_PASSWORD, 
                        ipaddress=POSTGRES_ADDRESS, 
                        port=POSTGRES_PORT, 
                        dbname=POSTGRES_DBNAME))
    #connect and creat engine
    cnx = create_engine(postgres_str)
    #inject dataframe to datatable
    df.to_sql(datatable, con=cnx, if_exists='append',index=False) 
# most frequet key word function return 2 dataframe: top 100 frequent word and post id associate with key word.                                  
def frequent_word(df,platform,sub_cat):
    #get the stop word dictionary 
    stop_words = set(stopwords.words('english')) 
    #lower the string
    df[['comment']]=df['comment'].str.lower()
    #tokenize the word
    df['token_comment'] = df.apply(lambda x: nltk.word_tokenize(x['comment']), axis=1)
    #specify stop words to be removed
    stop = stopwords.words('english')
    #get list of tokenize word without stopwords
    df['tokenized_without_stopwords'] = df['token_comment'].apply(lambda x:[word for word in x if word not in (stop)])
    #convert to the list to count
    a =list(df['tokenized_without_stopwords'])
    #remove all punctuation
    a=[str(w).translate(str.maketrans('', '', string.punctuation)).split() for w in a ]
    #convert to list
    a=list(itertools.chain.from_iterable(a))
    #count and return count value
    values, counts = np.unique(a, return_counts=True)
    #get today's date
    today = datetime.combine(date.today(), datetime.min.time())
    #the frequcey dataframe and sord by descending order
    Frequency=pd.DataFrame({'key':values,'count':counts,'datetime':today,'platform':platform,'sub_cat':sub_cat})\
    .sort_values(by=['count'],ascending=False).reset_index(drop=True)
    #define special drop value
    drop_values=["’",'nt','s']
    #drop special values
    Frequency=Frequency[Frequency['key'].isin(drop_values) == False]
    #get the top 100 frequent word
    Frequency_100=Frequency.head(100)
    #get the a list  with only words 
    key_string=list(Frequency_100['key'])
    #initial a empty data frame to store data
    Frequent_key_df = pd.DataFrame(columns=['post_id','platform','key'])
    #loop through all words 
    for i in key_string:
        #get rows contain certain keyword
        tmp=df[df.comment.str.contains(i)]
        #get the keyword 
        tmp['key']=i
        #subset wanted columns
        tmp=tmp[['post_id','platform','key']]
        #concate each rows from each interation
        Frequent_key_df=pd.concat([Frequent_key_df,tmp], ignore_index=True)    
    #drop duplicate based on post id and keyword
    Frequent_key_df=Frequent_key_df.drop_duplicates(subset=['post_id', 'key'])
    return Frequency_100,Frequent_key_df
def main(mytimer: func.TimerRequest) -> None:
    #list of facebook groups
    fb_group_list=['cirkulfamily','1319639691744530','1121674551544970']
    #range of date want to scrape
    date_range=7
    #list of hashtag in instagrame
    tag_list=['cirkulinfluencer','drinkcirkul','cirkul']
    cookie_json=['Cookie1.json','Cookie1.json','Cookie1.json']
    insta_sessionid=['48606361080%3ApU4c5ZHUBjsrNA%3A13','48606361080%3ApU4c5ZHUBjsrNA%3A13','48606361080%3ApU4c5ZHUBjsrNA%3A13']
    #loop through the facebook list
    for i in range(len(fb_group_list)):
        #define the plateform id for functions
        platform='facebook_group'
        #call the function
        out_put_df0=facebook_group_scraper(date_range=date_range,group_name=fb_group_list[i],cookie_json=cookie_json[i])
        #drop the text column as the varchar variable can not take too long string
        out_put_df01=out_put_df0.drop(columns=['comment'])
        #show the output shape
        print(out_put_df0.shape)
        #define the data table to be injected
        datatable='scrape_nlp_2021_varder00'
        #call the upload function to inject data
        uploader(df=out_put_df01,datatable=datatable)
        #call the frequent word function
        out_put_df10,out_put_df11=frequent_word(df=out_put_df0,platform=platform,sub_cat=i)
        #define the datatable to be injected
        datatable='word_frequency00'
        #call the upload function to inject data
        uploader(df=out_put_df10,datatable=datatable)
        #define the datatable to be injected
        datatable='postid_frequent_word00'
        #call the upload function to inject data
        uploader(df=out_put_df11,datatable=datatable)
    #loop through the facebook list
    for i in range(len(tag_list)):
        #define the plateform id for functions
        platform='instagram_hastag'
        #define the data table to be injected
        datatable='scrape_nlp_2021_varder00'
        #call the function
        out_put_df0=insta_scrapper(tag_name=tag_list[i],date_range=date_range,insta_sessionid=insta_sessionid[i])
        #drop the text column as the varchar variable can not take too long string
        out_put_df01=out_put_df0.drop(columns=['comment'])
        #show the output shape
        print(out_put_df0.shape)
        #call the upload function to inject data
        uploader(df=out_put_df01,datatable=datatable)
        #call the frequent word function
        out_put_df10,out_put_df11=frequent_word(df=out_put_df0,platform=platform,sub_cat=i)
        #define the datatable to be injected
        datatable='word_frequency00'
        #call the upload function to inject data
        uploader(df=out_put_df10,datatable=datatable)
        #define the datatable to be injected
        datatable='postid_frequent_word00'
        #call the upload function to inject data
        uploader(df=out_put_df11,datatable=datatable)
