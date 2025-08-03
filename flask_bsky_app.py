# app.py
# from backend.bluesky_client import fetch_posts
# from backend.graph_utils import build_networks, build_kpis, build_hashtag_csv_gexf, build_mentions_csv_gexf
# from backend.db_utils import mysql_connect #, mysql_create_tables, load_from_mysql, save_to_mysql #, sql_delete_df, sql_save_df, sql_read_df
# from backend.db_utils import validate_user #, mysql_get_searches
from backend.config import CONFIG, DEMO_HANDLE, DEMO_APP_PWD, HOME, MAX_LIMIT 
from backend.context import Context, PageResources, handle_context
from backend.my_render_template import my_render_template
from backend.decorators import login_required

import backend.route_folders
import backend.downloads 
import backend.auth_login_logout

from flask import Flask, render_template, request, session #, flash, redirect, url_for
# from flask import Blueprint, send_file, make_response
# from flask import send_from_directory
import pandas as pd
import os
# import io
# from io import BytesIO
# import networkx as nx
import uuid
import traceback
from datetime import datetime

app = Flask(__name__)

app.secret_key = CONFIG["SECRET_KEY"]

app.register_blueprint(backend.downloads.downloads)
app.register_blueprint(backend.route_folders.route_folders)
app.register_blueprint(backend.auth_login_logout.auth_login_logout)

# print(app.url_map)


# Parameters
class Parameters:
    handle = ''
    app_pwd = ''
    mode = ''           
    query = ''
    username = ''
    limit = 0
    search_id = ''
    session_id = ''
    
    def read_parameters():
        Parameters.handle = request.values.get('handle', '')
        Parameters.app_pwd = request.values.get('password', '')
        Parameters.mode = request.values.get('mode', '')            
        Parameters.query = request.values.get('query', '')
        Parameters.username = request.values.get('username', '')
        Parameters.limit = int(request.values.get('limit', '0'))
        Parameters.search_id = request.values.get('search_id', '')
        Parameters.session_id = request.values.get('session_id', '')


# build_session
@login_required
def build_session(handle, mode, query, username, limit, cached, search_id):
    session = {
            'handle': handle,
            'mode': mode,
            'query': query,
            'username': username,
            'limit': limit,
            'cached': cached,
            'search_id': search_id
        }   
    return session


# handle post method
@login_required
def handle_post():
    try:
        ok = True

        # leggo i paramertri
        Parameters.read_parameters()            
        
        # Salvo i parametri nel Context
        Context.handle =Parameters.handle 
        #Context.app_pwd =Parameters.app_pwd 
        Context.mode =Parameters.mode 
        Context.query =Parameters.query  
        Context.username =Parameters.username
        Context.limit =Parameters.limit  

        if Parameters.search_id and Parameters.session_id:
            Context.search_id = Parameters.search_id
            Context.session_id = Parameters.session_id

            Context.cached = True
        else:
            Context.cached = False
            Context.timestamp = datetime.now()  # o datetime.now() per ora locale
            Context.search_id = str(uuid.uuid4())

        #print(f"home handle={Parameters.handle}, mode={Parameters.mode}, query={query}, limit={limit}, cached={cached}")
#        print(f"{Parameters}")
        print(f"Context.query {Context.query} - Context.username {Context.username}")
    
        if not Context.handle or Context.handle.lower() == 'demo' or Context.handle.lower() == DEMO_HANDLE.lower():
            Context.app_pwd = DEMO_APP_PWD          # NO CONTEXT
            Context.handle = DEMO_HANDLE

        # costruisco la sessione
        Context.sessione = build_session(Context.handle, Context.mode, Context.query, Context.username, 
                                         Context.limit, Context.cached, Context.search_id)
        print(f"home {Context.sessione}")
        
        ok = Context.limit and Context.limit > 0 and Context.limit < MAX_LIMIT
        ok = ok and (Context.mode=='Hashtag' or Context.mode=='User')
        ok = ok and (Context.mode=='Hashtag' and Context.query) or (Context.mode=='User' and Context.username)

    except Exception as e:
        traceback.print_exc()  # stampa lo stacktrace completo su stdout
        ok = False
        
    return ok
    

# route index.html /
@app.route('/index.html', methods=["GET", "POST"])
@app.route('/', methods=["GET", "POST"])
@login_required
def home():
    # read context
    Context.read_context()

    # handle post
    print(f"home.request.method {request.method}") 
    if request.method == "POST": 
        if handle_post():
            my_render_template(HOME)            
  
    searches = handle_context()

    # close mysql connection
    Context.conn.close()
    
    return my_render_template( HOME, kpis=PageResources.kpis, top10=PageResources.top10, mentions_graph=PageResources.graph_mentions, 
                              hashtags_graph=PageResources.graph_hashtags, activity=PageResources.activity,
                              posts=PageResources.posts.to_dict(orient='records'), sessione=Context.sessione,
                              searches=searches)


# route .html
@app.route('/<page>.html', methods=["GET", "POST"])
@login_required
def render_html_page(page):
    print(f"render_html_page {page}")
    try:
        # read context
        Context.read_context()       

        # handle post
        print(f"home.request.method {request.method}") 
        if request.method == "POST": 
            handle_post()
        
        print(f"render_html_page search_id {Context.search_id}")
        
        if Context.session_id:
                   
            searches = handle_context()        

            # close mysql connection
            Context.conn.close()

            return my_render_template( f"{page}.html", kpis=PageResources.kpis, top10=PageResources.top10, 
                              mentions_graph=PageResources.graph_mentions, 
                              hashtags_graph=PageResources.graph_hashtags, activity=PageResources.activity,
                              posts=PageResources.posts.to_dict(orient='records'), sessione=Context.sessione, searches=searches )
        else:
            return my_render_template(f"{page}.html", posts=[])
    except:
        traceback.print_exc()  # stampa lo stacktrace completo su stdout
        # Se il file non esiste, torna un 404 personalizzato o il default
        return my_render_template('404.html'), 404        


# route progress
from flask import jsonify
@app.route('/fetch_progress')
@login_required
def fetch_progress():
    return jsonify({'progress': session.get('fetch_posts_progress', 0)})


if __name__ == '__main__':
    os.makedirs("static/networks", exist_ok=True)
    app.run(debug=True)