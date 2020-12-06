import logging
import requests
import json
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from datetime import datetime
import azure.functions as func
from time import time 

global serviceClient
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        data = req.get_json()
        tenentId = data['tenentId']
        contentType = data['contentType']
        grantType = data['grantType']
        clientId = data['clientId']
        clientSecret = data['clientSecret']
        resource = data['resource']
        endPointUrl = data['endPointUrl'] + tenentId + "/oauth2/token"
        workSpaceId = data['workSpaceId']
        logAnalyticsUrl = data['logAnalyticsUrl']
        logQuery = data['logQuery']
        accountName = data['accountName']
        accountKey = data['accountKey']
        stageContainerName = data['stageContainerName']
        stageFolderName = data['stageFolderName']
        oauthTokenResponseData = json.loads(GetOAuthAccessToken(contentType, grantType, clientId, clientSecret, resource, endPointUrl))
        if(oauthTokenResponseData['StatusCode'] == 200):
            logAnalyticsResponse = json.loads(RetrieveLogs(oauthTokenResponseData['AccessToken'], workSpaceId, logAnalyticsUrl, logQuery, accountName, stageContainerName, stageFolderName, accountKey))
            return func.HttpResponse(
                json.dumps(logAnalyticsResponse),
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                json.dumps(oauthTokenResponseData),
                mimetype="application/json",
                status_code=200
            )
    except Exception as ex:
        return func.HttpResponse(
            json.dumps(str(ex)),
            mimetype="application/json",
            status_code=500
        )
    

def GetOAuthAccessToken(contentType, grantType, clientId, clientSecret, resource, endPointUrl):

    url = endPointUrl
    payload='grant_type=' + grantType + '&client_id=' + clientId + '&client_secret=' + clientSecret + '&resource=' + resource
    headers = {
        'Content-Type': contentType
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    jsonResponse = json.loads(response.text)
    # logging.info("--------Access Token Response---------")
    # logging.info(jsonResponse)
    try:
        return json.dumps({
            'AccessToken' : jsonResponse['access_token'], 
            'Message' : "OAuth Access token has been retrieved successfully.",
            'StatusCode' : 200
        })
    except Exception as ex:
        return json.dumps({
            'AccessToken' : '', 
            'Message' : str(ex),
            'StatusCode' : 500
        })

def RetrieveLogs(accessToken, workSpaceId, baseUrl, logQuery, accountName, stageContainerName, stageFolderName, accountKey):
    logAnalyticsUrl = baseUrl + workSpaceId + "/query"
    try:
        url = logAnalyticsUrl
        payload="{\"query\":" + '"' + logQuery + '"' + "}"
        headers = {
            'Authorization': 'Bearer ' + accessToken,
            'Content-Type': 'application/json'
        }
        # logging.info(payload)
        response = requests.request("POST", url, headers=headers, data=payload)
        # jsonResponse = json.loads(response.text)
        stageDataUploadResponse = json.loads(UploadFileADLGen2(accountName, response.text, stageContainerName, stageFolderName, accountKey))
        # logging.info(type(stageDataUploadResponse))
        return json.dumps({
            # 'Data' : jsonResponse,
            "FileName" : stageDataUploadResponse["FileName"],
            'Message' : "Query data has been extracted successfully.",
            'StatusCode' : 200
        })
    except Exception as ex:
        return json.dumps({
            'Data' : '', 
            'Message' : str(ex),
            'StatusCode' : 500
        })
        
def UploadFileADLGen2(accountName, outputData, stageContainerName, stageFolderName, accountKey):
    serviceClient = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", accountName), credential=accountKey)
    if(serviceClient.get_file_system_client(stageContainerName) is not None):
        try:
            serviceClient.create_file_system(stageContainerName)
        except Exception as ex:
            pass
        fileSystemClient = serviceClient.get_file_system_client(stageContainerName)
        if(fileSystemClient.get_directory_client(stageFolderName) is not None):
            try:
                fileSystemClient.create_directory(stageFolderName)
            except Exception as ex:
                pass
        existingdirectoryClient = fileSystemClient.get_directory_client(stageFolderName)
        subDirectoryName = str(datetime.now().strftime("%m-%d-%Y"))
        if(existingdirectoryClient.get_sub_directory_client(subDirectoryName) is not None):
            try:
                existingdirectoryClient.create_sub_directory(subDirectoryName)
            except Exception as ex:
                pass
        directoryClient = fileSystemClient.get_directory_client(stageFolderName + "/" + subDirectoryName)
        storageFileName = "logfile-" + str(int(time() * 1000)) + ".json"
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
                "FileName" : storageFileName,
                "Message" : "New file has been uploaded successfully. File Name is- " + str(fileClient.path_name),
                "StatusCode" : 200
            })
