from . import views
from django.contrib import admin
from django.urls import path
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.gap_report, name='gap_report'),
]+ static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)