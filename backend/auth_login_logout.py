# auth_login_logout
from flask import Blueprint, redirect, render_template, request, url_for
from flask import session
from backend.my_render_template import my_render_template

from backend.config import CONFIG, DEMO_HANDLE, DEMO_APP_PWD, HOME, MAX_LIMIT 
from backend.db_utils import mysql_connect, validate_user


auth_login_logout = Blueprint('auth_login_logout', __name__)


# route auth-login
@auth_login_logout.route("/auth-login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        
        # Valida l'utente
        conn = mysql_connect(CONFIG["MYSQL_HOST"], CONFIG["MYSQL_USER"], CONFIG["MYSQL_PWD"], CONFIG["MYSQL_DB"])
        user = validate_user(username, password, conn)

        # close mysql connection
        conn.close()

        # Controlla se l'utente esiste e la password corrisponde
        if not user:
            return redirect(url_for("home"))
        else:
            session["user"] = username
            return redirect(url_for("home"))  # Reindirizza a index.html

    return render_template("auth-login.html")  # Usa auth-login.html


# route auth-logout
@auth_login_logout.route("/logout")
def logout():
    # Svuota la sessione
    session.clear()
    # Reindirizza al login
    return redirect(url_for("auth_login_logout.login"))
