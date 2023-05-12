"""
Django settings for hello_world project.

Generated by 'django-admin startproject' using Django 3.2.7.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.2/ref/settings/
"""
import os

import dotenv
from pathlib import Path
from common.utils import get_env_var
import dj_database_url
from datetime import timedelta


# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
try:
    dotenv.load_dotenv(BASE_DIR / '.env.testing')
except:
    dotenv.load_dotenv(BASE_DIR / '.env')
# dotenv.load_dotenv(BASE_DIR / '.env')
IS_HEROKU = False
IS_GITHUB = False
try:
    IS_HEROKU = get_env_var('IS_HEROKU')
except:
    pass
try:
    IS_GITHUB = get_env_var('IS_GITHUB')
except:
    pass


# this part will be executed if IS_POSTGRESQL = False
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
    }
}
MAX_CONN_AGE = 600
DJANGO_SECRET_KEY = get_env_var('DJANGO_SECRET_KEY')
SECRET_KEY=get_env_var('SECRET_KEY')
EMAIL_HOST_PASSWORD = get_env_var('EMAIL_PASSWD')
SCRAPFLY_KEY = get_env_var('SCRAPFLY_KEY')
ST_APP_KEY = get_env_var('ST_APP_KEY')
SALESFORCE_CONSUMER_KEY = get_env_var('SALESFORCE_CONSUMER_KEY')
SALESFORCE_CONSUMER_SECRET = get_env_var('SALESFORCE_CONSUMER_SECRET')
GOOGLE_CLIENT_ID=get_env_var('GOOGLE_CLIENT_ID')
# Celery Configuration
# REDIS_URL = os.environ.get('REDIS_URL')
if IS_HEROKU or IS_GITHUB:
    DEBUG = False
    CELERY_RESULT_BACKEND = get_env_var('REDIS_URL')
    BASE_FRONTEND_URL = 'https://app.ismycustomermoving.com'
    BASE_BACKEND_URL = 'https://is-my-customer-moving.herokuapp.com'
    CLIENT_ORIGIN_URL="https://app.ismycustomermoving.com"
    if IS_HEROKU:
        DATABASES["default"] = dj_database_url.config(
            conn_max_age=MAX_CONN_AGE, ssl_require=True)
    else:
        DATABASES["default"]["TEST"] = DATABASES["default"]
    CELERY_BROKER_URL = "{}?ssl_cert_reqs={}".format(
        CELERY_RESULT_BACKEND, "CERT_NONE",
)
else:
    DEBUG = True
    CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
    CELERY_BROKER_URL = CELERY_RESULT_BACKEND
    BASE_FRONTEND_URL = 'http://localhost:3000'
    BASE_BACKEND_URL = 'http://localhost:8000'
    CLIENT_ORIGIN_URL="http://localhost:3000"
    
ALLOWED_HOSTS = ['*']
SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = get_env_var('GOOGLE_CLIENT_ID')
SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = get_env_var('GOOGLE_CLIENT_SECRET')
SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE = ['email', 'profile']
SOCIAL_AUTH_GOOGLE_OAUTH2_AUTH_EXTRA_ARGUMENTS = {'access_type': 'offline'}


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_rest_passwordreset',
    'rest_framework',
    'corsheaders',
    'accounts',
    'payments',
    'data',
    'djstripe',
    'social_django',    
    'rest_framework_social_oauth2',
    'oauth2_provider'

]

MIDDLEWARE = [
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'csp.middleware.CSPMiddleware',
]

# Auth user
AUTH_USER_MODEL = "accounts.CustomUser"

ROOT_URLCONF = 'config.urls'

# CORS_ALLOW_ALL_ORIGINS = True
CORS_ALLOWED_ORIGINS = [
    CLIENT_ORIGIN_URL,

    # STRIPE
    "https://3.18.12.63",
    "https://3.130.192.231",
    "https://13.235.14.237",
    "https://13.235.122.149",
    "https://18.211.135.69",
    "https://35.154.171.200",
    "https://52.15.183.38",
    "https://54.88.130.119",
    "https://54.88.130.237",
    "https://54.187.174.169",
    "https://54.187.205.235",
    "https://54.187.216.72"
]
CORS_ALLOW_METHODS = [
    "DELETE",
    "GET",
    "OPTIONS",
    "PATCH",
    "POST",
    "PUT",
]

CORS_ALLOW_HEADERS = [
    "accept",
    "accept-encoding",
    "authorization",
    "content-type",
    "dnt",
    "origin",
    "user-agent",
    "x-csrftoken",
    "x-requested-with",
]
ROOT_URLCONF = 'config.urls'
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(BASE_DIR, 'templates')
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

# Password validation
# https://docs.djangoproject.com/en/4.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

PASSWORD_HASHERS = [
    # python -m pip install argon2-cffi
    # https://docs.djangoproject.com/en/3.2/topics/auth/passwords/#using-argon2-with-django
    'django.contrib.auth.hashers.Argon2PasswordHasher',
    'django.contrib.auth.hashers.PBKDF2PasswordHasher',
    'django.contrib.auth.hashers.PBKDF2SHA1PasswordHasher',
    'django.contrib.auth.hashers.BCryptSHA256PasswordHasher',
]

# Internationalization
# https://docs.djangoproject.com/en/4.1/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.1/howto/static-files/

STATIC_URL = '/static/'
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'
STATIC_ROOT = BASE_DIR / 'staticfiles'

STATICFILES_DIRS = [
    BASE_DIR / 'static',

]

# Default primary key field type
# https://docs.djangoproject.com/en/3.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
# Default primary key field type
# https://docs.djangoproject.com/en/4.1/ref/settings/#default-auto-field


# Oauth2
LOGIN_URL='/admin/login/'



SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_SECONDS = 31536000

CSP_FRAME_ANCESTORS = "'none'"

REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'accounts.utils.CustomAuthentication'
    ],
    'DEFAULT_SCHEMA_CLASS': 'rest_framework.schemas.coreapi.AutoSchema',
}
SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(minutes=60),
    'REFRESH_TOKEN_LIFETIME': timedelta(days=1),
    'ROTATE_REFRESH_TOKENS': True,
    'BLACKLIST_AFTER_ROTATION': True,
    'UPDATE_LAST_LOGIN': False,

    'ALGORITHM': 'HS256',
    'SIGNING_KEY': SECRET_KEY.encode(),
    'VERIFYING_KEY': None,
    'AUDIENCE': None,
    'ISSUER': None,
    'JWK_URL': None,
    'LEEWAY': 0,

    'AUTH_HEADER_TYPES': ('Bearer',),
    'AUTH_HEADER_NAME': 'Authorization',
    'USER_ID_FIELD': 'id',
    'USER_ID_CLAIM': 'user_id',
    'USER_AUTHENTICATION_RULE': 'rest_framework_simplejwt.authentication.default_user_authentication_rule',

    'AUTH_TOKEN_CLASSES': ('rest_framework_simplejwt.tokens.AccessToken',),
    'TOKEN_TYPE_CLAIM': 'token_type',
    'TOKEN_USER_CLASS': 'rest_framework_simplejwt.models.TokenUser',

    'JTI_CLAIM': 'jti',

    'SLIDING_TOKEN_REFRESH_EXP_CLAIM': 'refresh_exp',
    'SLIDING_TOKEN_LIFETIME': timedelta(minutes=60),
    'SLIDING_TOKEN_REFRESH_LIFETIME': timedelta(days=1),
}


STRIPE_TEST_SECRET_KEY = get_env_var('STRIPE_SECRET_KEY_TEST')
STRIPE_LIVE_SECRET_KEY = get_env_var('STRIPE_SECRET_KEY')
STRIPE_LIVE_MODE = False

DJSTRIPE_WEBHOOK_SECRET = "whsec_N8LMT9fUTcrtlEBvKaHLKTnXWLE2uybj"
DJSTRIPE_FOREIGN_KEY_TO_FIELD = "id"

DJSTRIPE_WEBHOOK_VALIDATION='retrieve_event'

# CELERY_BROKER_URL = CELERY_RESULT_BACKEND + "?ssl_cert_reqs=CERT_NONE"
CELERY_RESULT_BACKEND = CELERY_BROKER_URL
CELERY_TIMEZONE = 'US/Central'
CELERYD_TASK_TIME_LIMIT= 10
CELERY_TASK_RESULT_EXPIRES = 10

CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            "hosts": [get_env_var('REDIS_URL')],
        },
    },
}

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": get_env_var('REDIS_URL'),
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            "SSL_CERT_REQS": "CERT_NONE",
            "CONNECTION_POOL_KWARGS": {
                "ssl_cert_reqs": "CERT_NONE"
            },
        }
    }
}
# "CONNECTION_POOL_KWARGS": {
            #     "ssl_cert_reqs": ssl.CERT_NONE,
            # },

# EMAIL_BACKEND = env('DJANGO_EMAIL_BACKEND', default='django.core.mail.backends.console.EmailBackend')
EMAIL_HOST = "smtp.gmail.com" # Your SMTP Provider or in this case gmail
EMAIL_PORT = 587
EMAIL_USE_SSL = False
EMAIL_USE_TLS = True
EMAIL_HOST_USER = get_env_var('EMAIL')
ACCOUNT_EMAIL_VERIFICATION = 'none'
#assigned at the beginning
# EMAIL_HOST_PASSWORD