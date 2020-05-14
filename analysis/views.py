from django.shortcuts import render
from django.http import HttpResponse
from subprocess import run, PIPE, STDOUT
import sys
import os
import csv
from io import StringIO
import pandas as pd
from django.template import loader
from django.template.defaulttags import register
from django.conf import settings

...
@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)


# Create your views here.


def index(request):
    return render(request, 'analysis/index.html')


def execscript(request, id):
    column_index = {
        "1": "ecomsite",
        "2": "country",
        "3": "time_in_hour",
        "4": "language",
        "5": "category",
        "6": "hashtagText",
        "7": "ecomsite",
        "8": "ecomsite",
        "9": "screen_name",
        "10": "year"
    }
    script_file = os.path.join(settings.BASE_DIR, 'query-script/script.py')
    output = run([sys.executable, script_file,
                  id], shell=False, stdout=PIPE, stderr=STDOUT)
    foldername = 'query' + id
    rootdir = 'data/output/files/' + foldername
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            ext = os.path.splitext(file)[-1].lower()
            if (ext == '.csv'):
                filepath = rootdir+'/' + file
    queryOutput = pd.read_csv(filepath)
    result = queryOutput.set_index(column_index.get(id)).to_dict().get('count')
    return render(request, 'analysis/output.html', {'id': id, 'data': result, 'image_path': 'images/'+id+'.png'})
