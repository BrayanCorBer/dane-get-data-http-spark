from pyspark.sql import SparkSession
import requests
import json
import re


def run():
    spark = SparkSession.Builder().appName('test').getOrCreate()

    dataframe = spark.read.csv(
        'dataframe_instituciones.csv', header=True, inferSchema=True)

    dataframe.unpersist(blocking=True)

    direcciones = dataframe.select('DIRECCIÓN')

    return direcciones

# Transform the directions in a differents formats for url


def first_transform(d):
    # create the list for differents variables in the url
    rows = ['3', '20']
    aproxs = ['1', '0']
    pares = ['PAR', 'IMPAR']
    texts = [d.replace(' ', '%'), d.replace(' ', '%20')]
    data = ''

    # Get all url values as posible
    for row in rows:
        for aprox in aproxs:
            for par in pares:
                for text in texts:
                    # url construction
                    url = f'https://geoportal.dane.gov.co/laboratorio/serviciosjson/buscador/searchAddress.php?address={text}&dpto=11&mpio=001&pimpar={par}&rows={row}&aprox={aprox}'

                    response = requests.get(url)

                    # try the get a json file usefull
                    try:
                        data_json = response.json()['rows']
                        if data == '' or data == 'La dirección ingresada no es valida':
                            if len(data_json) == 1:
                                data = data_json
                    except Exception as err:
                        data = 'La dirección ingresada no es valida'

    return data

# In some case the direction needs a letter jpin to the respective number


def second_transform(txt):
    d = {}
    x = txt.split()

    for i in range(0, len(x)):
        if re.search(r'^[A-Z]$', x[i]):
            d[i] = x[i]

    for a in d:
        for i in range(0, len(x)):
            if a == i:
                x[i-1] += d[a]

    count = 0
    for a in d:
        for i in range(0, len(x)):
            if a == i:
                x.pop(a-count)
                count += 1

    txt = ' '.join(x)
    return txt
# in the case that the direction contains cardinal points this need transform to a first letter


def third_transform(txt):
    x = txt.split()

    for i in range(0, len(x)):
        if re.search(r'^[A-Z]{3,4}$', x[i]):
            x[i] = x[i][0]
    txt = ' '.join(x)
    return txt


# Iterate over de directions column and get the data
def transform(df):
    with open('test.txt', 'a') as file:
        for direction in df.collect():

            data = first_transform(direction[0])
            if data:
                file.write(f'{data}\n')
            else:
                data2 = second_transform(direction[0])
                new_direction = first_transform(data2)
                data = new_direction
                if data:
                    file.write(f'{data}\n')
                else:
                    data3 = third_transform(data2)
                    new_direction = first_transform(data3)
                    data = new_direction
                    if data:
                        file.write(f'{data}\n')
                    else:
                        file.write(f'noData\n')


if __name__ == '__main__':
    df = run()
    transform(df)
