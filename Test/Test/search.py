# -*- coding: utf-8 -*-

from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.shortcuts import render


# 表单
def search_form(request):
    return render_to_response('search_form.html')


# 接收请求数据
def search(request):
    request.encoding = 'utf-8'
    context = {}
    if 'q' in request.GET and request.GET['q']:
        context['error'] = "True"
        context['docs'] = [{
            "id": "https://www.google.com",
            "title": "EPCC",
            "comment": "Student in EPCC"
        },
            {
                "id": "https://www.epcc.ed.ac.uk/work-us",
                "title": "Home Page",
                "comment": "Accelerator, our on-demand computing service for business, brings leading edge supercomputing capability directly to your desktop. We are currently offering free trials of Accelerator. "
            }]
    else:
        context['docs'] = ['You submited a blank page.']
    return render(request, "index.html", context)