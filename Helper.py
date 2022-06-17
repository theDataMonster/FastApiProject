#import pandas
import pandas as pd
import datetime as DT


import Config as C

FILE_NAME= C.FILE_NAME

def getFullDF(FILE_NAME):
    df = pd.read_csv(FILE_NAME)
    return  df

def getFilteredDfByAmount(df,value,operator):
    if operator=="=":
        return df.loc[df['amount'] == float(value)]

    if operator ==">=":
        return df.loc[df['amount'] >= float(value)]

    if operator ==">":
        return df.loc[df['amount'] > float(value)]

    if operator =="<=":
        return df.loc[df['amount'] <= float(value)]

    if operator =="<":
        return df.loc[df['amount'] < float(value)]

def getFilteredDfByString(df,column_name,value):
    return df.loc[df['%s'%column_name] == "%s"%value]

def getFilteredDfByDate(df,value,operator):
    if operator=="=":
        return df.loc[df['date'] == DT.datetime.strptime(value,"%m/%d/%Y").date()]

    if operator ==">=":
        return df.loc[df['date'] >= DT.datetime.strptime(value,"%m/%d/%Y").date()]

    if operator ==">":
        return df.loc[df['date'] > DT.datetime.strptime(value,"%m/%d/%Y").date()]

    if operator =="<=":
        return df.loc[df['date'] <= DT.datetime.strptime(value,"%m/%d/%Y").date()]

    if operator =="<":
        return df.loc[df['date'] < DT.datetime.strptime(value,"%m/%d/%Y").date()]

def getSortedData(df,value,order):
    attributesList=value.split(",")
    if order.lower()=='asc':
        return df.sort_values(by=attributesList, ascending=True)
    elif order.lower()=='desc':
        return  df.sort_values(by=attributesList, descending=True)
    else
    return df

def getSparseData(df,fields):
    fieldList = fields.split(",")
    return  df[fieldList]

def getAggregatedData(df,field):
    if field in df.columns.values:
        return df.groupby(['%s'%field])['amount'].sum().reset_index()
    return -1



