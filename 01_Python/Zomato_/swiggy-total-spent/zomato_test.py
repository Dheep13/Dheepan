import requests
import json
import sys
from rich import print
import os


HEADERS = {
    'Host': 'www.zomato.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.zomato.com/',
    'Content-Type': 'application/json'
}

cookies = {
    'csrf': 'b2a961610fc609d4048d7f970438735a',
    'zat': 'tdvEnswbg8nKFeZqt70DF7mfF7s17kOKnn7sQfuWpDE.GebUfDmgsoTNtT7YbwA2xRcka2akStlcL5gybffyk0U',
    'ak_bmsc': 
'6BA739B93F5B8919040BC7AC5BB4BA46~000000000000000000000000000000~YAAQQekyF+DmpP+FAQAAoqW9ARJNOJ8gTv4iLa473w8lVoi7kdk80KSvI5glgB+bjSBS2dYdu4J5xiLG88qdCeEVPIPalwFxVmn11KMp5mrde8bSKNZVx60LoP7z65tocJgc8ClLdR7XmgMKX9+3LyJlcHcIi272CSab+mglEElR8VQ7VctGyimhDdKnLU1jy2Az3UFbAywFOZh8539ZGjX1rdw20hUqvPYekyNjSrP+eXY9h8tPRLPbpdH1QjTfCzOehN1i09A/7zbJd8EuIxgEIb4f1wCEed5YRSV/5jMH4K7MRT22nlgS6oiVJsUPcL3QHL2CBz1nKWX9yGFDsQ8YYYFBU9ssO+zvK5i60PA8AQvDDRxHV7WPIrta2HEuyoDGc4Zx+ReIEQ==',
    'fbcity': '3',
    '_ga': 'GA1.1.826093824.1675066454',
    'G_ENABLED_IDPS': 'google',
    '_ga_2XVFHLPTVP': 'GS1.1.1675066455.1.1.1675067706.0.0.0',
    'zl': 'en',
    '_fbp': 'fb.1.1675066456399.584157092',
    '_gcl_au': '1.1.2117478609.1675066453',
    '_gid': 'GA1.2.49244015.1675066454',
    'AKA_A2': 'A',
    'AWSALBTG':
'BWjEayMuUgWqdITevW61D+otqzR3Sjm6FzhVdNUBlO6pJj1S8Nx8k94cZgru0WMvZUK09k41xO880jnn5Om5yrldt2ZdQaZ9GrXb50Zdvc0dE8mXST4H8UkAAOeKLH4Ywsqs6uv0DIneqaiDOoA+025Q8lTMMyJ7mYznsD9Y8bW4',
    'AWSALBTGCORS':
'BWjEayMuUgWqdITevW61D+otqzR3Sjm6FzhVdNUBlO6pJj1S8Nx8k94cZgru0WMvZUK09k41xO880jnn5Om5yrldt2ZdQaZ9GrXb50Zdvc0dE8mXST4H8UkAAOeKLH4Ywsqs6uv0DIneqaiDOoA+025Q8lTMMyJ7mYznsD9Y8bW4',
    'bm_sv':
'FE8A313E2376909CB7C13AAB6C91BB95~YAAQv2PUF7s2QuyFAQAAAq7QARJOyzrZ2biqJEigX1f+92g44QKNpxBbE9bH4JxMtOTypYBlR9sVZQfCOkJW07j8c+C2HkGfIyUe99gf37QZ6IM4f485pvs7GKLiNShBdmBAbgTgr0z51qAkV2Pzqytjj2xkXEI+ovLscFm8EC5i4ZlppThadqzPbso7p8p1czQr29YGs/QIjTyH3xRH9Y5qHUY13W73S2zncbRTHZsM6CwNtHShSpjS7e7+8ZcQ9w==~1',
    'cid': '2c4e3ed9-0308-4d16-a237-3a5c99f7e944',
    'fbtrack': '5cc94bcd09eeb65e38a306fa1e63214a',
    'fre': '0',
    'hy-en': '1',
    'locus':
'%7B%22addressId%22%3A0%2C%22lat%22%3A19.017656%2C%22lng%22%3A72.856178%2C%22cityId%22%3A3%2C%22ltv%22%3A3%2C%22lty%22%3A%22city%22%2C%22fetchFromGoogle%22%3Afalse%2C%22dszId%22%3A77482%2C%22fen%22%3A%22Mumbai%22%7D',
    'ltv': '3',
    'lty': '3',
    'PHPSESSID': '50249c76546a35cbd319c5a0d8103b2e',
    'rd': '1380000',
    'ttaz': '1677658484'}
current_page=1
GET_ORDERS_URL='https://www.zomato.com/webroutes/user/orders?page='
URL = GET_ORDERS_URL+str(current_page).strip()
s = requests.Session()
r = s.get(URL, headers=HEADERS, cookies=cookies)
resp = json.loads(r.text)
print(resp)