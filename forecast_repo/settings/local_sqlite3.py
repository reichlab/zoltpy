# NB: must set the following before importing from base

DEBUG = True

SECRET_KEY = 'i9)wcfth2$)-ggdx2n-z9ek4o4o759cpgo)_gk(oen8713g%to'

from .base import *


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
    }
}
