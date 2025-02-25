import requests

BASE_API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

page_number = 1
while True:
    
    # enviando requisição GET com parâmetro (página)
    params = {'page': page_number}
    response = requests.get(BASE_API_URL, params=params)
    page_data = response.json()

    # se o json estiver vazio
    if not page_data:
        break

    print(page_data)
    page_number += 1

    # limitando o nº de páginas para testes
    if page_number > 2:
      break