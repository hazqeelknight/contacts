# Import settings based on environment
import os
from decouple import config

# Determine which settings to use
ENVIRONMENT = config('DJANGO_SETTINGS_MODULE', default='config.settings.development')

if 'development' in ENVIRONMENT:
    from .development import *
elif 'production' in ENVIRONMENT:
    from .production import *
else:
    from .base import *