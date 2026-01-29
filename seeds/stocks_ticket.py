tickers = [
    "NVDA","AAPL","MSFT","AMZN","META","AVGO","GOOGL","GOOG","TSLA","NFLX",
    "PLTR","COST","AMD","ASML","CSCO","AZN","MU","TMUS","SHOP","APP",
    "LIN","PEP","ISRG","LRCX","INTU","PDD","QCOM","AMAT","INTC","ARM",
    "BKNG","AMGN","KLAC","TXN","GILD","ADBE","PANW","HON","CRWD","CEG",
    "ADI","ADP","DASH","MELI","SBUX","SNPS","VRSK","WBA","BIIB","REGN",
    "CSX","MRVL","JD","ZM","DOCU","IDXX","ORLY","MCHP","ROST","MAR",
    "VRTX","DXCM","ILMN","ATVI","KLAC","CTSH","NXPI","EXC","CDNS","SGEN",
    "CTAS","FAST","SWKS","EBAY","EA","TCOM","BIDU","EA","LULU","SIRI",
    "MNST","XEL","WDC","PAYX","TTWO","CTAS","AEP","FISV","VRSN","PCAR",
    "MELI","ALGN","CDW","ASML","MRNA","SPLK","OKTA","ANSS","EBAY","KHC"
]

with open("stocklist.txt", "w") as f:
    for ticker in tickers:
        f.write(ticker + "\n")

print("stocklist.txt created with 100 tickers.")