from flask import Blueprint, make_response, send_file, redirect, url_for, flash
from io import BytesIO
import traceback
import pandas as pd

from backend.graph_utils import build_mentions_csv_gexf,build_hashtag_csv_gexf
from backend.context import Context, PageResources, handle_context
from backend.db_utils import load_from_mysql
from backend.decorators import login_required

downloads = Blueprint('downloads', __name__)

@downloads.route('/download/mentions.csv')
@login_required
def download_mentions_csv():
    Context.read_context()
    handle_context()
    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)
    csv_edges, _ = build_mentions_csv_gexf(df)
    Context.conn.close()
    response = make_response(csv_edges)
    response.headers.set('Content-Disposition', 'attachment', filename='mentions_edge_list.csv')
    response.headers.set('Content-Type', 'text/csv')
    return response

@downloads.route('/download/mentions.gexf')
@login_required
def download_mentions_gexf():
    Context.read_context()
    handle_context()
    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)
    _, gexf_data = build_mentions_csv_gexf(df)
    Context.conn.close()
    return send_file(gexf_data, mimetype='application/octet-stream', as_attachment=True, download_name='mentions_network.gexf')

@downloads.route('/download/hashtags.csv')
@login_required
def download_hashtags_csv():
    Context.read_context()
    handle_context()
    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)
    csv_edges, _ = build_hashtag_csv_gexf(df)
    Context.conn.close()
    response = make_response(csv_edges)
    response.headers.set('Content-Disposition', 'attachment', filename='hashtags_edge_list.csv')
    response.headers.set('Content-Type', 'text/csv')
    return response

@downloads.route('/download/hashtags.gexf')
@login_required
def download_hashtags_gexf():
    Context.read_context()
    handle_context()
    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)
    _, gexf_data = build_hashtag_csv_gexf(df)
    Context.conn.close()
    return send_file(gexf_data, mimetype='application/octet-stream', as_attachment=True, download_name='hashtags_network.gexf')

@downloads.route('/download/session_posts.json')
@login_required
def download_session_df_json():
    Context.read_context()
    handle_context()
    df = load_from_mysql(Context.session_id, Context.conn, Context.search_id)
    Context.conn.close()
    buffer = BytesIO(df.to_json(orient='records').encode('utf-8'))
    buffer.seek(0)
    return send_file(buffer, mimetype='application/json', as_attachment=True, download_name='session_posts.json')

@downloads.route('/download/collected_posts.csv')
@login_required
def download_posts_csv():
    try:
        if PageResources.posts is None or PageResources.posts.empty:
            flash("Nessun post disponibile per il download.", "error")
            return redirect(url_for("home"))
        posts_copia = PageResources.posts.copy()
        posts_copia['text'] = posts_copia['text'].str.replace(r'[\r\n\u2028\u2029]+', '<br>', regex=True)
        csv_data = posts_copia.to_csv(index=False, encoding='utf-8-sig', quotechar='"')
        response = make_response(csv_data)
        response.headers.set('Content-Disposition', 'attachment', filename='collected_posts.csv')
        response.headers.set('Content-Type', 'text/csv')
        return response
    except Exception:
        traceback.print_exc()
        flash("Errore durante il download dei posts.", "error")
        return redirect(url_for("home"))

@downloads.route('/download/collected_posts.xlsx')
@login_required
def download_posts_xlsx():
    try:
        if PageResources.posts is None or PageResources.posts.empty:
            flash("Nessun post disponibile per il download.", "error")
            return redirect(url_for("home"))
        posts_copia = PageResources.posts.copy()
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            posts_copia.to_excel(writer, index=False, sheet_name='Collected Posts')
        buffer.seek(0)
        response = make_response(buffer.getvalue())
        response.headers.set('Content-Disposition', 'attachment', filename='collected_posts.xlsx')
        response.headers.set('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        return response
    except Exception:
        traceback.print_exc()
        flash("Errore durante il download dei posts.", "error")
        return redirect(url_for("home"))
