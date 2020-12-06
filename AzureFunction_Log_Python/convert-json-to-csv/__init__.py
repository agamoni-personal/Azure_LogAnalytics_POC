import logging
import azure.functions as func
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from datetime import datetime
from time import time 
import pandas as pd
import json
from flatten_json import flatten

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    global queryResponse
    try:
        data = req.get_json()
        if(data['logType'] == 'AzureActivity'):
            queryResponse = AzureActivityLogPreprocess(data['stageContainerName'], data['stageFolderName'], data['accountName'], data['accountKey'], data['preprocessContainerName'], data['preprocessFolderName'], data['stageFileName'])
        return func.HttpResponse(
            json.dumps(queryResponse),
            mimetype="application/json",
            status_code=200
        )
    except Exception as ex:
        return func.HttpResponse(
            json.dumps(str(ex)),
            mimetype="application/json",
            status_code=200
        )

def AzureActivityLogPreprocess(stageContainerName, stageFolderName, accountName, accountKey, preprocessContainerName, preprocessFolderName, stageFileName):
    GetDataLakeServiceClient(accountName, accountKey)
    stageOutputData = ReadStageFile(stageContainerName, stageFolderName, stageFileName)
    preprocessedOutputData = ConvertJsontoCsv(stageOutputData)
    queryResponse = json.loads(UploadPreprocessFileADLGen2(preprocessedOutputData, preprocessContainerName, preprocessFolderName, stageFileName))
    return queryResponse

def UploadPreprocessFileADLGen2(outputData, containerName, folderName, fileName):
    if(serviceClient.get_file_system_client(containerName) is not None):
        try:
            serviceClient.create_file_system(containerName)
        except Exception as ex:
            pass
        fileSystemClient = serviceClient.get_file_system_client(containerName)
        if(fileSystemClient.get_directory_client(folderName) is not None):
            try:
                fileSystemClient.create_directory(folderName)
            except Exception as ex:
                pass
        existingdirectoryClient = fileSystemClient.get_directory_client(folderName)
        # subDirectoryName = str(datetime.now().strftime("%m-%d-%Y"))
        # if(existingdirectoryClient.get_sub_directory_client(subDirectoryName) is not None):
        #     try:
        #         existingdirectoryClient.create_sub_directory(subDirectoryName)
        #     except Exception as ex:
        #         pass
        directoryClient = fileSystemClient.get_directory_client(folderName)
        # storageFileName = "logfile-" + str(int(time() * 1000)) + ".csv"
        storageFileName = fileName
        if(directoryClient.get_file_client(storageFileName) is not None):
            try:
                directoryClient.create_file(storageFileName)
            except Exception as ex:
                pass
        fileClient = directoryClient.get_file_client(storageFileName)
        fileContents = outputData
        fileClient.append_data(data=fileContents, offset=0, length=len(fileContents))
        fileClient.flush_data(len(fileContents))
        if(directoryClient.get_file_client(storageFileName) is not None):
            return json.dumps({
                "Message" : "New file has been uploaded successfully. File Name is- " + str(fileClient.path_name),
                "StatusCode" : 200
            })

def ConvertJsontoCsv(jsonRequestData):
    jsonRequestData = set(jsonRequestData)
    columnsHead = []
    rows = []
    try:
        for column in jsonRequestData['tables'][0]['columns']:
            columnsHead.append(column['name'])
        for outerIndex in range(0, len(jsonRequestData['tables'][0]['rows'])):
            newRow = []
            for innerIndex in range(0, len(jsonRequestData['tables'][0]['rows'][outerIndex])-1):
                rowValue = str(jsonRequestData['tables'][0]['rows'][outerIndex][innerIndex]).replace("\r\n", "").replace("\n", "")
                newRow.append(rowValue)
            rows.append(newRow)
        df = pd.DataFrame (rows , columns = columnsHead)
        output = df.to_json(orient ='records') 
        # output = df.to_csv (index_label="idx", encoding = "utf-8", sep = "|")
        return output
    except Exception as ex:
        return str(ex)
    
def ReadStageFile(containerName, folderName, fileName):
    file_system_client = serviceClient.get_file_system_client(file_system=containerName)
    directory_client = file_system_client.get_directory_client(folderName)
    file_client = directory_client.get_file_client(fileName)
    download = file_client.download_file()
    downloaded_bytes = download.readall().decode()
    return json.loads(downloaded_bytes)

def flattenNestedJson(jsonString):
    flat_json = flatten(unflat_json)
    return flat_json

def GetDataLakeServiceClient(accountName,accountKey):
    try:
        global serviceClient
        serviceClient = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", accountName), credential=accountKey)
    except Exception as ex:
        pass