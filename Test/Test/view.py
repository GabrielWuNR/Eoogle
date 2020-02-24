# -*- coding: utf-8 -*-

#from django.http import HttpResponse
from django.shortcuts import render

def toIndex(request):
    context = {}
    return render(request, "index.html", context)
