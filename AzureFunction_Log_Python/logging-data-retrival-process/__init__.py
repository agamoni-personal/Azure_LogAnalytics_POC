import logging
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from time import time 
from datetime import datetime
import pandas as pd
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        data = req.get_json()
        GetDataLakeServiceClient(data['accountName'], data['accountKey'])
        global fileSystemClient
        if(serviceClient.get_file_system_client(data['containerName']) is not None):
            try:
                serviceClient.create_file_system(data['containerName'])
            except Exception as ex:
                pass
        fileSystemClient = serviceClient.get_file_system_client(data['containerName'])
        if(fileSystemClient.get_directory_client(data['pipelineName']) is not None):
            try:
                fileSystemClient.create_directory(data['pipelineName'])
            except Exception as ex:
                pass
        existingdirectoryClient = fileSystemClient.get_directory_client(data['pipelineName'])
        storageFileName = "pipelinelog-" + str(int(time() * 1000)) + ".csv"
        if(existingdirectoryClient.get_file_client(storageFileName) is not None):
            try:
               existingdirectoryClient.create_file(storageFileName) 
            except Exception as ex:
                pass
        fileClient = existingdirectoryClient.get_file_client(storageFileName)
        columnsHead = ["TimeStamp", "LogMessage", "StatuCode", "PipelineName", "PipelineRunId"]
        rowsValue = [[datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), data['message'], data['statusCode'], data['pipelineName'], data['pipelineRunId']]]
        df = pd.DataFrame (rowsValue , columns = columnsHead)
        output = df.to_csv (index_label="idx", encoding = "utf-8", sep = "|")
        fileContents = output
        fileClient.append_data(data=fileContents, offset=0, length=len(fileContents))
        fileClient.flush_data(len(fileContents))
        return func.HttpResponse(
            json.dumps({
                "Message" : "New log file has been uploaded",
                "StatusCode" : 200
            }),
            mimetype="application/json",
            status_code=200
        )    
    except Exception as ex:
        return func.HttpResponse(
            str(ex),
            mimetype="application/json",
            status_code=500
        )

def GetDataLakeServiceClient(accountName,accountKey):
    try:
        global serviceClient
        serviceClient = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", accountName), credential=accountKey)
    except Exception as ex:
        pass