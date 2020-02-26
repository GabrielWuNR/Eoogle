# -*- coding: utf-8 -*-

from django.http import HttpResponse
from django.shortcuts import render
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage, InvalidPage
from Test import deal_user_query
from Test import cache
query = deal_user_query.parse_search()
res_cache = cache.cache_all_search()
# 接收请求数据
def search(request):
    request.encoding = 'utf-8'
    context = {}
    if 'q' in request.GET and request.GET['q']:
        page = request.GET.get('page')
        context['key']=request.GET['q']
        if(context['key']!=res_cache.previous):
            res_cache.cache_list = query.getSearch(context['key'])
            res_cache.previous = context['key']
        paginator = Paginator(res_cache.cache_list, 10)
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