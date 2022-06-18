# This is the Helper File which contains the relevant functions required to execute our process successfully.
import pandas as pd
import datetime as DT


import Config as C

FILE_NAME= C.FILE_NAME

#Get the full data by directly reading the input file
def getFullDF(FILE_NAME):
    df = pd.read_csv(FILE_NAME)
    return  df

#Get filtered data based on amount
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

#Get filtered data based on columns which store the text values
def getFilteredDfByString(df,column_name,value):
    return df.loc[df['%s'%column_name] == "%s"%value]

#Get filtered data based on date column
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

#Sort the data by multiple field values entered by comma
def getSortedData(df,value,order):
    attributesList=value.split(",")
    if order.lower()=='asc':
        return df.sort_values(by=attributesList, ascending=True)
    if order.lower()=='desc':
        return  df.sort_values(by=attributesList, descending=True)

#Get Sparse data based only on certains fields
def getSparseData(df,fields):
    fieldList = fields.split(",")
    return  df[fieldList]

#Get aggregated amount data based on other column values
def getAggregatedData(df,field):
    if field in df.columns.values:
        return df.groupby(['%s'%field])['amount'].sum().reset_index()
    return -1

def checkColumnIsNotPresent(df,fields):
    attributesList = fields.split(",")
    if set(attributesList).issubset(set(df.columns.values)) == False:
        return  True
    return  False



