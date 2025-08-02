# app.py
from flask import Flask, render_template, request, session
from backend.bluesky_client import fetch_posts
from backend.graph_utils import build_networks, build_kpis, build_hashtag_csv_gexf, build_mentions_csv_gexf
import pandas as pd
import os
from flask import Blueprint, send_file, make_response
import io
import networkx as nx
import uuid
from backend.db_utils import mysql_connect, mysql_create_tables, load_from_mysql, save_to_mysql #, sql_delete_df, sql_save_df, sql_read_df
from backend.db_utils import mysql_get_searches
from io import BytesIO
from flask import send_from_directory
import traceback
from datetime import datetime
import json

app = Flask(__name__)
app.secret_key = "DFGGRE£$%ERDFG$£%"

#Legge le impostazioni SQL
with open("mysql_conn.json") as f:
    config = json.load(f)

#MYSQL_HOST = 'localhost'
#MYSQL_USER = 'nodiux'
#MYSQL_PWD = 'Medoro90'
#MYSQL_DB = 'nodiux'

#MYSQL_HOST = 'nodiux.mysql.pythonanywhere-services.com'
#MYSQL_USER = 'nodiux'
MYSQL_PWD = 'Medoro90'
#MYSQL_DB = 'nodiux$nodiux'

HOME = 'index.html'

DEMO_HANDLE = 'nodiux.bsky.social'
DEMO_APP_PWD = '42fc-xq6u-p2qh-tlm5'

MAX_LIMIT=10000

print(f"Config: {config}")

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
        Context.conn = mysql_connect(config["MYSQL_HOST"], config["MYSQL_USER"], MYSQL_PWD, config["MYSQL_DB"])
        mysql_create_tables(Context.conn)

        Context.sessione = read_session_data()
        ( 
            Context.handle, Context.mode, Context.query, Context.username,
            Context.limit, Context.cached, Context.search_id
        )  = parse_session_data(Context.sessione)

        Context.session_id = session.get('session_id', '')

        print(f'''read_context handle={Context.handle}, mode={Context.mode}, query={Context.query}, 
              limit={Context.limit}, cached={Context.cached}''')
        print(f"read_context session_id={Context.session_id}")
        print(f"{Context.sessione}")
        #print(f"read_context context={Context}")


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
        print('load_from_sql')        
        if Context.search_id:
            PageResources.df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)
        else:
            print('load_from_sql no search_id')
            PageResources.df = pd.DataFrame([])

    def build_networks():
        PageResources.graph_mentions, PageResources.graph_hashtags = build_networks(PageResources.df)
    
    def build_kpis():
        PageResources.kpis, PageResources.top10, PageResources.activity, PageResources.posts = build_kpis(PageResources.df)


# my_render_template
def my_render_template(pagina, **kwargs):
    # Costruisci un dizionario base con sessione e default comuni
    base_context = {
        'sessione': {
                'username': session.get('username', ''),
                'mode': session.get('mode', 'Hashtag'),
                'query': session.get('query', ''),
                'limit': session.get('limit', 100)
            },
        'kpis': {
                "posts": 0,
                "users": 0,
                "mentions": 0,
                "hashtags": 0
            },
        'top10': {
                "active_users" : {},
                "emojis" : {},
                "hashtags" : {},
                "mentions" : {}
            },
        'mentions_graph': None,
        'hashtags_graph': None,
        'activity': None,
        'posts': [],
        'error': None
    }

    # Aggiorna con eventuali override passati al momento della chiamata
    base_context.update(kwargs)
    #print(f"{base_context}")

    # Renderizza
    return render_template(pagina, **base_context)


# parse_session_data
def parse_session_data(sessione):
    handle = sessione.get('handle', '')
    mode = sessione. get('mode', '')
    username = sessione.get('username', '')
    query = sessione.get('query', '')
    limit = sessione.get('limit', 0)
    cached = sessione.get('cached', False)
    search_id = sessione.get('search_id', '')
    
    return handle, mode, query, username, limit, cached, search_id


# read_session_data
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
    #print(f"read_session_data {sessione}")
    return sessione


# build_session
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
    

# read page resources
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
                      request.remote_addr, Context.timestamp, Context.search_id)        
        Context.cached = True   
   
    # graphsuri1973!
    
    #print(df.to_json(orient='records')[:10000])

    print(f'{Context.handle}')
    searches=mysql_get_searches(Context.handle, Context.conn)

    #print(df.to_json(orient='records')[:10000])

    Context.sessione['cached'] = Context.cached
    session['session_data'] = Context.sessione

    return searches

# route index.html /
@app.route('/index.html', methods=["GET", "POST"])
@app.route('/', methods=["GET", "POST"])
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


# route downloads
@app.route('/download/mentions.csv')
def download_mentions_csv():
    Context.read_context()
    searches = handle_context()

    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)  
    print(f"session_id:{Context.session_id} search_id:{Context.search_id} query:{Context.query}")
    csv_edges, gexf_data = build_mentions_csv_gexf(df)
    
    # close mysql connection
    Context.conn.close()

    # Genera i dati
    response = make_response(csv_edges)    
    response.headers.set('Content-Disposition', 'attachment', filename='mentions_edge_list.csv')
    response.headers.set('Content-Type', 'text/csv')
    return response

@app.route('/download/mentions.gexf')
def download_mentions_gexf():
    Context.read_context()
    searches = handle_context()

    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)  
    print(f"session_id:{Context.session_id} search_id:{Context.search_id} query:{Context.query}")
    csv_edges, gexf_data = build_mentions_csv_gexf(df)
    
    # close mysql connection
    Context.conn.close()
    
    return send_file(gexf_data, mimetype='application/octet-stream', as_attachment=True, download_name='mentions_network.gexf')

@app.route('/download/hashtags.csv')
def download_hashtags_csv():
    Context.read_context()
    searches = handle_context()

    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)  
    print(f"session_id:{Context.session_id} search_id:{Context.search_id} query:{Context.query}")
    csv_edges, gexf_data = build_hashtag_csv_gexf(df)

    # close mysql connection
    Context.conn.close()
       
    # Genera i dati
    response = make_response(csv_edges)
    response.headers.set('Content-Disposition', 'attachment', filename='hashtags_edge_list.csv')
    response.headers.set('Content-Type', 'text/csv')
    return response

@app.route('/download/hashtags.gexf')
def download_hashtags_gexf():
    Context.read_context()
    searches = handle_context()

    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)  
    print(f"session_id:{Context.session_id} search_id:{Context.search_id} query:{Context.query}")
    csv_edges, gexf_data = build_hashtag_csv_gexf(df)

    # close mysql connection
    Context.conn.close()
    
    return send_file(gexf_data, mimetype='application/octet-stream', as_attachment=True, download_name='hashtags_network.gexf')

@app.route('/download/session_posts.json')
def download_session_df_json():    
    Context.read_context()
    searches = handle_context()

    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)  
    print(f"session_id:{Context.session_id} search_id:{Context.search_id} query:{Context.query}")

    # close mysql connection
    Context.conn.close()
    
    df_json = df.to_json(orient='records')
    
    buffer = BytesIO(df_json.encode('utf-8'))
    buffer.seek(0)
    
    return send_file(buffer, mimetype='application/json', as_attachment=True, download_name='session_posts.json' )


# route folders/
@app.route('/img/<path:filename>')
def img(filename):
    return send_from_directory('templates/img', filename)
@app.route('/vendor/<path:filename>')
def vendor(filename):
    return send_from_directory('templates/vendor', filename)
@app.route('/css/<path:filename>')
def css(filename):
    return send_from_directory('templates/css', filename) 
@app.route('/js/<path:filename>')
def js(filename):
    return send_from_directory('templates/js', filename)
@app.route('/lib/bindings/<path:filename>')
def lib_bindings(filename):
    return send_from_directory('templates/lib/bindings', filename)
@app.route('/assets/avatars/<path:filename>')
def assets_avatars(filename):
    return send_from_directory('templates/assets/avatars', filename)
@app.route('/fonts/<path:filename>')
def fonts(filename):
    return send_from_directory('templates/fonts', filename)
@app.route('/assets/images/<path:filename>')
def assets_images(filename):
    return send_from_directory('templates/assets/images', filename)
@app.route('/.well-known/appspecific/<path:filename>')
def well_known_appspecific(filename):
    return send_from_directory('templates/.well-known/appspecific', filename)


if __name__ == '__main__':
    os.makedirs("static/networks", exist_ok=True)
    app.run(debug=True)