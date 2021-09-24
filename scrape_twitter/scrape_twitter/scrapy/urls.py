# -*- coding: utf-8 -*-
"""
Created on Wed Sep 22 12:50:28 2021

@author: Rakesh
"""

from django.urls import path

from . import views

urlpatterns = [
    path('retweets',views.retweets_data, name='retweets'),
    path('tweets', views.tweet_data, name='tweets')
]