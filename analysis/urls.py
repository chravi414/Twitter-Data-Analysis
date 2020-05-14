from django.urls import path
from .views import index, execscript
urlpatterns = [
    path('', index, name="index"),
    path('<str:id>', execscript, name='execscript')
]
