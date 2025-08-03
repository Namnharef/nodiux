# my_render_template

# my_render_template
from flask import render_template
from flask import session


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
