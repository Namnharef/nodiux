# route_folders.py
from flask import Blueprint, app, send_from_directory

route_folders = Blueprint('route_folders', __name__)

@route_folders.route('/img/<path:filename>')
def img(filename):
    return send_from_directory('templates/img', filename)
@route_folders.route('/vendor/<path:filename>')
def vendor(filename):
    return send_from_directory('templates/vendor', filename)
@route_folders.route('/css/<path:filename>')
def css(filename):
    return send_from_directory('templates/css', filename) 
@route_folders.route('/js/<path:filename>')
def js(filename):
    return send_from_directory('templates/js', filename)
@route_folders.route('/lib/bindings/<path:filename>')
def lib_bindings(filename):
    return send_from_directory('templates/lib/bindings', filename)
@route_folders.route('/assets/avatars/<path:filename>')
def assets_avatars(filename):
    return send_from_directory('templates/assets/avatars', filename)
@route_folders.route('/fonts/<path:filename>')
def fonts(filename):
    return send_from_directory('templates/fonts', filename)
@route_folders.route('/assets/images/<path:filename>')
def assets_images(filename):
    return send_from_directory('templates/assets/images', filename)
@route_folders.route('/.well-known/appspecific/<path:filename>')
def well_known_appspecific(filename):
    return send_from_directory('templates/.well-known/appspecific', filename)
