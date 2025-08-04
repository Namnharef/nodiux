# app.py
# from backend.bluesky_client import fetch_posts
# from backend.graph_utils import build_networks, build_kpis, build_hashtag_csv_gexf, build_mentions_csv_gexf
# from backend.db_utils import mysql_connect #, mysql_create_tables, load_from_mysql, save_to_mysql #, sql_delete_df, sql_save_df, sql_read_df
# from backend.db_utils import validate_user #, mysql_get_searches
import mysql
from backend.config import CONFIG, DEMO_HANDLE, DEMO_APP_PWD, HOME, MAX_LIMIT 
from backend.context import Context, PageResources, handle_context
from backend.db_utils import add_to_mysql, mysql_connect, mysql_get_searches, save_to_mysql
from backend.my_render_template import my_render_template
from backend.decorators import login_required

import backend.route_folders
import backend.downloads 
import backend.auth_login_logout

from flask import Flask, jsonify, render_template, request, session #, flash, redirect, url_for
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
                              searches=searches, active_search_id=session.get('active_search_id', None))


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
#from flask import jsonify
#@app.route('/fetch_progress')
#@login_required
#def fetch_progress():
#    return jsonify({'progress': session.get('fetch_posts_progress', 0)})


def get_search_info(search_id):
    print(f"get_search_info search_id={search_id}")
    conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM searches WHERE search_id = %s", (search_id,))
    result = cursor.fetchone()
    cursor.close()
    return result


def set_user_search_progress(user, search_id, status, progress=None):
    """
    Aggiorna lo stato e il progresso dell'utente per una ricerca asincrona.
    - Se status == 'start': inserisce una nuova riga con progress = 0.
    - Se status == 'completed': cancella la riga.
    - Altrimenti aggiorna la riga esistente con status e progress.
    
    Se progress non è passato, default a 0 per start, None per altri.
    """
    try:
        conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
        cursor = conn.cursor()
        
        if status == 'start':
            print(f"Setting progress for user {user}, search_id {search_id} to start")
            # Inserimento
            query = """
                INSERT INTO user_async_searches (username, search_id, status, progress)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (user, search_id, 'idle', 0))
            conn.commit()
            result = True
        
        elif status == 'completed':
            print(f"Setting progress for user {user}, search_id {search_id} to completed")
            # Cancellazione
            query = """
                DELETE FROM user_async_searches
                WHERE username = %s AND search_id = %s
            """
            cursor.execute(query, (user, search_id))
            conn.commit()
            result = cursor.rowcount > 0
        
        else:
            print(f"Updating progress for user {user}, search_id {search_id} to {status}, progress {progress}")
            # Se status è 'running' o 'error', aggiorna la riga esistente
            # Update
            # Se progress è None, tieni progress invariato oppure metti 0 di default
            if progress is None:
                # Opzionale: mantieni progress invariato oppure imposta a 0
                # Qui imposto 0 per sicurezza:
                progress = 0
            query = """
                UPDATE user_async_searches
                SET status = %s, progress = %s
                WHERE username = %s AND search_id = %s
            """
            cursor.execute(query, (status, progress, user, search_id))
            conn.commit()
            result = cursor.rowcount > 0
        
        cursor.close()
        conn.close()
        
        return result
    
    except Exception as e:
        print(f"DB error in set_user_search_progress: {e}")
        return False


def get_user_search_progress(user, search_id):
    print(f"Getting progress for user {user}, search_id {search_id}")
    try:
        conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT status, progress
            FROM user_async_searches
            WHERE username = %s AND search_id = %s
        """
        cursor.execute(query, (user, search_id))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return {
                'status': result['status'],
                'progress': result['progress']
            }
        else:
            return {'status': 'completed','progress': 100}
        
    except Exception as e:
        print(f"DB error in get_user_search_progress: {e}")
        return {'status': 'completed','progress': 100}


def has_active_search(user):
    """
    Ritorna True se l'utente ha almeno una ricerca con status diverso da 'completed',
    altrimenti False.
    """
    try:
        conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
        cursor = conn.cursor()
        query = """
            SELECT 1
            FROM user_async_searches
            WHERE username = %s
        """
        cursor.execute(query, (user,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        return result is not None

    except Exception as e:
        print(f"DB error in has_active_search: {e}")
        return False


@app.route('/progress/<search_id>', methods=['GET'])
@login_required
def get_search_progress(search_id):
    print(f"get_search_progress search_id={search_id}")

    user = session.get('user')
    if not user:
        result = {'status': 'completed', 'progress': 100, 'searches': []}

    conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
    searches = mysql_get_searches(user, conn, search_id=search_id) or []

    progress_data = get_user_search_progress(user, search_id)
    if not progress_data:
        result = {'status': 'completed', 'progress': 100, 'searches': searches[:1]}  # Prendo massimo 1 elemento

    result = {'status': progress_data['status'], 'progress': progress_data['progress'], 'searches': searches[:1]}  # Prendo massimo 1 elemento

    print(f"get_search_progress result={result}")

    return jsonify(result)


@app.route('/search_more/<search_id>', methods=['POST'])
@login_required
def search_more(search_id):
    from threading import Thread
    from backend.bluesky_client import fetch_posts_async

    if has_active_search(session["user"]):
        return jsonify({'error': 'You already have an active search running.'}), 400

    Context.read_context()
    
    user = session["user"]

    print(f"search_more search_id={search_id}")

    # Recupera info da DB, es: username, password, query, mode, limit
    search = get_search_info(search_id)
    session_id = session.get('session_id')

    print(f"search_more search_id={search_id}, session_id={session_id}, search={search}")

    if not search:
        print(f"Search not found for search_id {search_id}")
        return jsonify({'error': 'Search not found'}), 404

    session['active_search_id'] = search_id
    session['search_progress'] = 0
    session['search_status'] = 'idle'

    print(f"active_search_id={session['active_search_id']}")

    def update_progress(done, total):
        print(f"Updating progress: {done}/{total}")
        progress = int((done / total) * 100)
        set_user_search_progress(user, search_id, "running", progress)

    def background_job():
        print(f"Starting background job for search_id={search_id}, session_id={session_id}")
        try:            
            set_user_search_progress(user, search_id, "start", 0)
            print(f"search: {search}")
            print(f"Context: {Context.handle}, {Context.app_pwd}, {search['mode']}, {search['query']}, {search['resultlimit']}")
            df, err = fetch_posts_async(
                username=DEMO_HANDLE,
                password=DEMO_APP_PWD,
                mode=search["mode"],
                query=search["query"],
                limit=search["resultlimit"],
                update_progress_callback=update_progress
            )
            if err:
                print(f"Error fetching posts: {err}")
                set_user_search_progress(user, search_id, "error", -1)
            else: 
                conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
                add_to_mysql(df, search['session_id'], conn, search_id)   
                set_user_search_progress(user, search_id, "completed", 100)
        except:
            traceback.print_exc()
            print(f"Error in background job for search_id={search_id}")
            set_user_search_progress(user, search_id, "error", -1)

    Thread(target=background_job).start()
    return jsonify({'status': 'started'})


@app.route('/clear_search_more/<search_id>', methods=['POST'])  # è preferibile POST per operazioni di cancellazione
@login_required
def clear_search_more(search_id):
    user = session.get('user')
    if not user:
        return jsonify({"error": "User not authenticated"}), 401

    try:
        conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
        cursor = conn.cursor()

        query = """
            DELETE FROM user_async_searches
            WHERE username = %s 
        """
        cursor.execute(query, (user,))
        conn.commit()
        rows_deleted = cursor.rowcount

        cursor.close()
        conn.close()

        if rows_deleted == 0:
            return jsonify({"error": "Search not found or already deleted"}), 404
        else:
            return jsonify({"success": True})
    except Exception as e:
        print(f"DB error in clear_search_more: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/remove/<search_id>', methods=['POST'])
@login_required
def remove_search(search_id):
    user = session.get('user')
    if not user:
        return jsonify({"error": "User not authenticated"}), 401

    try:
        conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
        cursor = conn.cursor()

        # Prima elimina da searches_cids (assumo che abbia username e search_id)
        query1 = """
            DELETE sc
            FROM searches_cids sc
            INNER JOIN searches s ON sc.search_id = s.search_id
            WHERE s.user = %s AND s.search_id = %s
        """
        cursor.execute(query1, (user, search_id))

        # Poi elimina da searches
        query2 = """
            DELETE FROM searches
            WHERE user = %s AND search_id = %s
        """
        cursor.execute(query2, (user, search_id))

        conn.commit()

        rows_deleted_searches = cursor.rowcount  # riferimento all'ultima query

        cursor.close()
        conn.close()

        if rows_deleted_searches == 0:
            return jsonify({"error": "Search not found or already deleted"}), 404

        return jsonify({"success": True})

    except Exception as e:
        print(f"DB error in remove_search: {e}")
        return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    os.makedirs("static/networks", exist_ok=True)
    app.run(debug=True)