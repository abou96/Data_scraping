from datetime import date
import re
from django.core.management.base import BaseCommand
#from django.utils import timezone
import pandas as pd
import datetime
import math
import re
#from sqlalchemy import create_engine
from mybudget_app.models import foot_mercato

import datetime
import time
from mybudget_app.models import Budget, monthly_table
import logging
from mybudget.model_tools import get_connection, dataframe_to_table, make_date_ready
import requests
from bs4 import BeautifulSoup

xl_origin = datetime.datetime(1899,12,30)
def iso_to_datetime(iso_date):

    return datetime.datetime(int(iso_date[:4]),int(iso_date[4:6]),int(iso_date[6:8]),)

#next step ajouter la date de publication et l'auteur
def scraping_foot_mercato(date, standings_url = "https://www.footmercato.net/"):
    data = requests.get(standings_url)
    #Scraping our first page with requests
    soup = BeautifulSoup(data.text)
    results = soup.select('article a')
    #Parsing our first page with requests
    links = list(set([r['href'] for r in results]))
    article_urls = [f"https://www.footmercato.net{l}" for l in links]
    print(len(article_urls))
    #############################################
    title_list = []
    article_lead_list = []
    import re

    allowlist = [
        'p'
        ]
    p_list = []
    noise = "\n', ' /\n\n\n                        ', ' /\n\n                    ', '\n"
    noise = "\n"
    for article_url in article_urls:
        try :
            data = requests.get(article_url)
        except:
            continue
        soup = BeautifulSoup(data.text)
        if len(soup.select('h1[class="title"]')) < 1:
            continue
        title = soup.select('h1[class="title"]')[0].text.replace("\n        ", "").replace("\n    ", "")
        
        ##########################################################""
        try :
            data = requests.get(article_url)
        except:
            continue
        soup = BeautifulSoup(data.text)
        if len(soup.select('h2[class="article__lead"]')) < 1:
            continue
        article__lead = soup.select('h2[class="article__lead"]')

            
        ###################################################################

        try :
            data = requests.get(article_url)
        except :
            continue
        soup = BeautifulSoup(data.text)
        text_elements = ' '.join([str(t) for t in soup.find_all(text=True) if t.parent.name in allowlist])
        #for i in range(len(text_elements)):
        text_elements = re.sub("[^a-zA-Z-À-ú-Â-û-0-9-.']", ' ', text_elements)
    
        p_list.append(text_elements)
        title_list.append(title)
        if len(article__lead) != 0:
            article__lead = article__lead[0].text.replace("\n        ", "").replace("\n    ", "")
            article_lead_list.append(article__lead)
        time.sleep(1)
    print('title_list', len(title_list))
    print('title_list', len(article_lead_list))
    print('title_list', len(p_list))
    if len(title_list) != len(article_lead_list) or len(title_list) != len(article_lead_list):
        
        raise Exception ("bad scraping")
    articles_info = list(zip(title_list, article_lead_list, p_list))
    # Converting lists of tuples into
    # pandas Dataframe.
    df_article = pd.DataFrame(articles_info, columns = ['title', 'article_lead', 'content'])
    if len(df_article) != 0:
        df_article = df_article.fillna('vide')
        df_article['content'] = df_article['content'].fillna('vide')

        # df_article['title'].replace('', np.nan, inplace=True)
        # df_article.dropna(title=['Tenant'], inplace=True)
        # df_article = df_article.reset_index(drop=True)
        print(df_article)
    df_article.to_csv("h_compte/article_foot_mercato" + date + ".csv")
    return df_article


class Command(BaseCommand):
    help = "Insert datas from CSV"

    def add_arguments(self, parser):
        super().add_arguments(parser)
        
        parser.add_argument(
                    '--dry',
                    action='store_true',
                    dest='dry',
                    default=False,
                    help='Dry run, do not alter database..',
                )


        parser.add_argument('csv_in', type=str)
        parser.add_argument('dateiso', type=str)
        #parser.add_argument('outfolder')


    def handle(self, *args, **options):
        csv_in = options['csv_in']
        #outfolder = options['outfolder']
        dateiso = options['dateiso']
        dry = options['dry']
        date = iso_to_datetime(dateiso)
        print(csv_in)
        #df = pd.read_csv(csv_in, sep= ';', error_bad_lines=False)
        df = scraping_foot_mercato(dateiso)
        print(df)
        print(len(df))

        df['date'] = date
        
        print(df)
        #stockage data to db_table
        #dd = df[df.duplicated(['date'], keep=False)].groupby(['date']).last()

        # for d, _ in dd.iterrows():
        #     #d = str(d)
        #     d = d.date()
        #     print('dddddddd', d)
        #     if not dry and not insert:
        #         make_date_ready(Budget, d)

        if not dry:
            now = datetime.datetime.now()
            df['created'] = now
            df['modified'] = now
            dataframe_to_table(df, dry, 'mybudget_app_foot_mercato', disable_trigger = True)
        # #stockage data to db_table
        # engine = create_engine('sqlite:///DB_name')
        # df.to_sql(Budget._meta.db_table, if_exists='replace', con=engine, index=False)


