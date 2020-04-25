
import pandas as pd

def create_paths(symbols):
    paths = []

    for symbol in symbols:
        path = "/Users/alperoner/Desktop/PMP/Historical_Datas/{}_history.csv".format(symbol)
        paths.append(path)

    return paths

def view(paths):
    for path in paths:
        print(path)
        data = pd.read_csv(path)
        print(data.head())


symbols = ["TSLA", "AAPL"]
paths = create_paths(symbols)
view(paths)