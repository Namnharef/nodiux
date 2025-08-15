# context.py
from backend.bluesky_client import fetch_posts
from backend.db_utils import mysql_connect
from backend.decorators import login_required
from backend.config import CONFIG, DEMO_HANDLE, DEMO_APP_PWD, HOME, MAX_LIMIT 
from backend.db_utils import mysql_connect, mysql_create_tables, load_from_mysql, save_to_mysql #, sql_delete_df, sql_save_df, sql_read_df
from backend.db_utils import mysql_get_searches, validate_user
from backend.graph_utils import build_kpis, build_networks

from flask import Flask, render_template, request, session, flash, redirect, url_for
# from flask import Blueprint, send_file, make_response
from flask import send_from_directory
import pandas as pd
import os
# import io
# from io import BytesIO
# import networkx as nx
import uuid
import traceback
from datetime import datetime


# Context
class Context:
    conn = None
    sessione = None
    handle = ''
    mode = ''
    query = ''
    username = ''
    limit = 0
    cached = False
    search_id = ''
    session_id = ''
    app_pwd = ''

    @staticmethod
    def read_context():
        # Connessione
        Context.conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
        mysql_create_tables(Context.conn)

        Context.sessione = read_session_data()
        print(f"1 {Context.sessione}")

        ( 
            Context.handle, Context.mode, Context.query, Context.username,
            Context.limit, Context.cached, Context.search_id
        )  = parse_session_data(Context.sessione)

        print(f"2 {Context.sessione}")

        Context.session_id = session.get('session_id', '')

        print(f"3 {Context.sessione}")

        print(f'''read_context handle={Context.handle}, mode={Context.mode}, query={Context.query}, 
              limit={Context.limit}, cached={Context.cached}''')
        print(f"read_context session_id={Context.session_id}")

        #print(f"read_context context={Context}")


# Page resources
class PageResources:
    graph_mentions = None
    graph_hashtags = None
    kpis = None
    top10 = None
    activity = None
    posts = None
    df = None
    error = None

    def fetch_posts():        
        PageResources.df, PageResources.error = fetch_posts(Context.handle, Context.app_pwd, Context.mode, 
                                                            Context.query if Context.mode == 'Hashtag' else Context.username, 
                                                            Context.limit)

    def load_from_sql():
        print(f'load_from_sql user: {session.get("user")}, session_id: {Context.session_id}, search_id: {Context.search_id}')        
        if Context.search_id:
            PageResources.df = load_from_mysql(session.get("user"), Context.conn, Context.search_id)
        else:
            print('load_from_sql no search_id')
            PageResources.df = pd.DataFrame([])

    def build_networks():
        PageResources.graph_mentions, PageResources.graph_hashtags = build_networks(PageResources.df)
    
    def build_kpis():
        PageResources.kpis, PageResources.top10, PageResources.activity, PageResources.posts = build_kpis(PageResources.df)


# read_session_data
@login_required
def read_session_data():
    sessione = session.get('session_data', {
            'username': '',
            'mode': '',
            'query': '',
            'username': '',
            'limit': 0,
            'cached': False,
            'search_id': ''
        })
    print(f"read_session_data {sessione}")
    return sessione


# parse_session_data
@login_required
def parse_session_data(sessione):
    handle = sessione.get('handle', '')
    mode = sessione. get('mode', '')
    username = sessione.get('username', '')
    query = sessione.get('query', '')
    limit = sessione.get('limit', 0)
    cached = sessione.get('cached', False)
    search_id = sessione.get('search_id', '')
    
    return handle, mode, query, username, limit, cached, search_id


# read page resources
@login_required
def get_page_resources():
    posts=PageResources.posts = pd.DataFrame([])

    print(f"get_page_resources Context.cached {Context.cached}")

    if not Context.cached:
        # fetch posts
        PageResources.fetch_posts()                        
    else:
        PageResources.error = None
        PageResources.load_from_sql()

    if not PageResources.error:
        #print(f"home df.count={PageResources.df.count}")    

        # networks
        PageResources.build_networks()

        # kpis
        PageResources.build_kpis()


# handle context
@login_required
def handle_context():
    # session id
    if not Context.session_id:
        print("New context seassion_id")        
        Context.session_id = str(uuid.uuid4())
        session['session_id'] = Context.session_id  

        if not Context.handle or Context.handle.lower() == 'demo' or Context.handle.lower() == DEMO_HANDLE.lower():
            Context.app_pwd = DEMO_APP_PWD          # NO CONTEXT
            Context.handle = DEMO_HANDLE
            Context.cached = True

    print(f"session_id {Context.session_id}")
    print(f"search_id {Context.search_id}")

    # get page resources
    get_page_resources()

    # error
    if PageResources.error:
        print(f"home error={PageResources.error}")
        #return my_render_template(HOME, error=PageResources.error)   

    if not Context.cached:
        # save df to mysql        
        print("Saving to mysql ..... . . . . . . . .")
        save_to_mysql(PageResources.df, Context.handle, Context.session_id, Context.conn, Context.mode, 
              Context.query if Context.mode == 'Hashtag' else Context.username, Context.limit, 
              request.remote_addr, Context.timestamp, Context.search_id, session.get('user'))       
        Context.cached = True   
   
    # graphsuri1973!
    
    #print(df.to_json(orient='records')[:10000])

    print(f'{Context.handle}')
    searches = mysql_get_searches(session.get('user'), Context.conn, '')

    #print(df.to_json(orient='records')[:10000])

    Context.sessione['cached'] = Context.cached
    session['session_data'] = Context.sessione

    return searches
