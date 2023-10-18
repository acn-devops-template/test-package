"""deployment_helper utility package"""
__version__ = "0.1.1"  # will be overwritten by poetry_bumpversion

from django.db import connection

def show_user(request, username):
  with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM users WHERE username = '%s'" % username)
