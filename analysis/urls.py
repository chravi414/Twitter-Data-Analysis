from django.urls import path
from .views import index, runscript
urlpatterns = [
    path('', index, name="index"),
    path('<str:id>', runscript, name='runscript')
]
