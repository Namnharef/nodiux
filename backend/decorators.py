from flask import session, redirect, url_for, flash

# login_required decorator
from flask import redirect, url_for
from functools import wraps 
def login_required(f):
    @wraps(f)
    def wrapped_function(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("auth_login_logout.login"))  # se non è loggato, vai al login
        return f(*args, **kwargs)  # altrimenti, procedi
    return wrapped_function