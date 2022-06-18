# This file contains the services that will be deployed on the server.
# FastAPI is used here to expose the REST endpoints.
# Uvicorn is also required to host the webservices written in this file

from fastapi import FastAPI, Depends,status,HTTPException
import uvicorn
from typing import Union
import Helper
import  Config as C
import datetime as DT

FILE_NAME=C.FILE_NAME
dfExpanses = Helper.getFullDF(FILE_NAME)
dfExpanses['amount'] = dfExpanses['amount'].apply(lambda x: float(str(x)[:-1].replace(",","")))
dfExpanses['date'] = dfExpanses['date'].apply(lambda x: DT.datetime.strptime(x,"%m/%d/%Y").date())

app = FastAPI()

#Get all the raw data that is stored in the expanses.csv file
@app.get('/expansesData/')
async def root():
        dfResult= dfExpanses
        dfResult['date'] = dfResult['date'].apply(lambda x: DT.datetime.strftime(x, "%m/%d/%Y"))
        return dfResult.to_json()

# Filter our input data stored in expanses.csv file with the corresponding column names along with the relational operators
# like >=,=,etc. as listed in the Config.py file
# This allows multiple filters on our data.
@app.get('/expansesData/filter/')
async def read_item(amount: Union[float,None] = None,amount_flag: Union[str,None]= None, member_name: Union[str,None] = None, departments: Union[str,None] =None,project_name:Union[str,None]=None,date_param: Union[str,None]=None,date_flag: Union[str,None]=None):
        if amount==None and member_name==None and departments==None and project_name==None and date_param ==None:
            return dfExpanses.to_json()
        dfResult= dfExpanses
        if amount!=None:
            if amount_flag==None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f'Empty flag is not supported.')
            if amount_flag not in C.FLAG_LIST:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f'Flag {amount_flag} is not supported.')
            dfResult= Helper.getFilteredDfByAmount(dfResult,amount,amount_flag)

        if member_name!=None:
            dfResult = Helper.getFilteredDfByString(dfResult,"member_name",member_name)

        if departments!=None:
            dfResult = Helper.getFilteredDfByString(dfResult,"departments",departments)

        if project_name!=None:
            dfResult = Helper.getFilteredDfByString(dfResult,"project_name",project_name)
        if date_param != None:
            if date_flag==None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f'Empty flag is not supported.')
            if date_flag not in C.FLAG_LIST:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f'Flag {date_flag} is not supported.')
            dfResult = Helper.getFilteredDfByDate(dfResult,date_param,date_flag)
            dfResult['date'] = dfResult['date'].apply(lambda x: DT.datetime.strftime(x, "%m/%d/%Y"))

        return dfResult.to_json()

#Sort the data by fileds separated by comma. The order also needs to be specified to be asc or desc
@app.get('/expansesData/sort/')
async def read_item(fields: str, order: str):
    if order not in C.ORDER_LIST:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f'Invalid order type passed. Only asc and desc supported')
    if Helper.checkColumnIsNotPresent(dfExpanses,fields):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f'Invalid field name passed. Only relevant columns are supported')

    dfResult = Helper.getSortedData(dfExpanses, fields, order)
    dfResult['date'] = dfResult['date'].apply(lambda x: DT.datetime.strftime(x, "%m/%d/%Y"))
    return dfResult.to_json()

#Get aggregated amount based on other columns received through request parameter
@app.get('/expansesData/aggregates/')
async def read_item(field: str):
    if field not in dfExpanses.columns.values:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,detail=f'Invalid field name passed. Only relevant columns are supported')

    dfResult=Helper.getAggregatedData(dfExpanses, field)
    if field == 'date':
        dfResult['date'] = dfResult['date'].apply(lambda x: DT.datetime.strftime(x, "%m/%d/%Y"))
    return dfResult.to_json()

#Get data for specific fields only
@app.get('/expansesData/fields/')
async def read_item(fields: str):
    #attributesList = fields.split(",")
    if Helper.checkColumnIsNotPresent(dfExpanses,fields):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f'Invalid field name passed. Only relevant columns are supported')

    dfResult=Helper.getSparseData(dfExpanses, fields)
    if 'date' in fields.split(","):
        dfResult['date'] = dfResult['date'].apply(lambda x: DT.datetime.strftime(x, "%m/%d/%Y"))
    return dfResult.to_json()



if __name__ == '__main__':
    uvicorn.run(app)#, port=8080, host='0.0.0.0')  # run our Flask app