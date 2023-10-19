import json

with open('./test.txt', 'r') as file1:
    lines = file1.readlines()
    for line in lines:
        data = json.loads(line)
        if data:
            L = data['L']
            G = data['G']
        else:
            L = None
            G = None
        with open('./data.csv', 'a') as file2:
            file2.write(f'{L},{G}\n')

        