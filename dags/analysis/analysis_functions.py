import gzip
import os
import requests
import pandas as pd
from urllib.parse import urlparse
import numpy as np
import tldextract



def is_homepage(url):
    parsed_url = urlparse(url)
    return (parsed_url.path == '' or parsed_url.path == '/') and parsed_url.query == '' and parsed_url.fragment == ''

def get_homepage(url):
    parsed_url = urlparse(url)
    homepage = f"{parsed_url.scheme}://{parsed_url.netloc}"
    return homepage

def get_subsection(url):
    parsed_url = urlparse(url)
    return parsed_url.path + ('?' + parsed_url.query if parsed_url.query else '') + ('#' + parsed_url.fragment if parsed_url.fragment else '')

def get_country(url):
    ext = tldextract.extract(url)
    tld = ext.suffix
    tld = tld.split(".")[-1]
    if len(tld) != 2:
        tld = 'UNKNOWN'
    return tld

def create_check_ad(row):
    if row['category'] == 'UNKNOWN':
        return np.random.choice([True, False])
    else:
        return None
