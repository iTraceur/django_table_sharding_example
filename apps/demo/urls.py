# -*- coding: utf-8 -*-
from django.urls import path

from . import views

urlpatterns = [
    path('user/', views.UserView.as_view(), name='user'),
    path('log/', views.LogView.as_view(), name='log'),
]
