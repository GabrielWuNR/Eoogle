# -*- coding: utf-8 -*-

from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.shortcuts import render
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage, InvalidPage


# 表单
def search_form(request):
    return render_to_response('search_form.html')


# 接收请求数据
def search(request):
    request.encoding = 'utf-8'
    context = {}
    if 'q' in request.GET and request.GET['q']:
        page = request.GET.get('page')
        context['key']=request.GET['q']
        list = [{
            "id": "https://www.google.com",
            "title": "EPCC",
            "comment": "Student in EPCC"
        },
            {
                "id": "https://www.epcc.ed.ac.uk/work-us",
                "title": "Home Page",
                "comment": "Accelerator, our on-demand computing service for business, brings leading edge supercomputing capability directly to your desktop. We are currently offering free trials of Accelerator. "
            }]
        paginator = Paginator(list, 1)
        try:
            message = paginator.page(page)
        except PageNotAnInteger:
            message = paginator.page(1)
        except InvalidPage:
            return HttpResponse("Page is not found.")
        except EmptyPage:
            message = paginator.page(paginator.num_pages)
        context['error'] = "True"
        context['docs'] = message
    else:
        context['docs'] = ['You submited a blank page.']
    return render(request, "index.html", context)