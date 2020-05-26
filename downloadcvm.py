#funcionou
import pandas as pd
import requests,threading,time,urllib,requests
import io
from datetime import datetime, timedelta, date
start = datetime.today()

def get_last_day_previus_month(current_month_date):
    """
    Method that return the lest day of previus month 
    
    """
    
    first_day_current_month = current_month_date.replace(day=1)
    previus_month = first_day_current_month - timedelta(days=1)
    
    return previus_month
    
def make_urls(url,date= date.today(),extencion=None):
    """
    Method that make urls to use in requests
    return is a list with urls with done contain dates 
    (current_date until 3 years before)
    """

    urls = []
    current_date = date
    aux_current_date = current_date
    
    while int(aux_current_date.strftime("%Y")) > (int(current_date.strftime("%Y")) - 4):
        urls.append(url+aux_current_date.strftime("%Y%m")+"."+extencion)
        aux_current_date = get_last_day_previus_month(aux_current_date)
    
    return urls

requests_list = list() 

# def save_in_requests_list(func):
#     global requests_list
    
#     print("ta na lista")
#     requests_list.append(func)


def make_request(url):
    """
    Method that make request
    Return is the content of request
    """
    global requests_list
    
    print("Request to {}".format(url))
    try:
        
        read_table = requests.get(str(url)).content
        requests_list.append(read_table)

        
    except Exception as e:
        print("We had the exception {exception} in {url}".format(exception=str(e),url=str(url)))
        
def transform_to_table(value):
    """ 
    Method that transform strings to tables
    Return is a spark dataframe
    
    """
    
    print(value)

def start_requests(urls):
    """ Method that make async requests"""
    for url in urls:
        t = threading.Thread(target=make_request,args=(str(url),))
        t.start()


    

urls = make_urls(url=r"http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_",extencion="csv")
start_requests(urls) 

while len(requests_list) < len(urls):

    time.sleep(1)



print(len(requests_list))
for i in requests_list:
    t_B = threading.Thread(target=transform_to_table, args=(i,))

print("finalizou")
print("Time was start:{} and end:{}".format(start,datetime.today))