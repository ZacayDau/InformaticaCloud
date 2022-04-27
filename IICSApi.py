import pathlib
import requests
import json
import os
from datetime import date, datetime
import time

import Utility
from ConfigurationFile import ConfigurationFile

import logging.handlers
import os
from datetime import datetime

cn=ConfigurationFile()
WORKINGDIR=cn.data["WORKINGDIR"]
log_dir =cn.data["log_dir"]
timestampNow = datetime.now()

class JSONFormatter(logging.Formatter):
   def __init__(self):
      super().__init__()
   def format(self, record):
      record.msg = json.dumps(record.msg)
      return super().format(record)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
loggingStreamHandler = logging.StreamHandler()
loggingStreamHandler = logging.FileHandler(log_dir+"/"+"logs.json",mode='a') #to save to file
loggingStreamHandler.setFormatter(JSONFormatter())
logger.addHandler(loggingStreamHandler)



#log=LogFileWriter("IICS",log_dir)

class IICSApi:

    def __init__(self, user,password,baseApiUrl,operationUrl):
        self.user = user
        self.password = password
        self.baseApiUrl = baseApiUrl
        self.sessionID=self.login(operationUrl)



    def login(self,operationUrl):

        print("login...")
        logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": "login..."})
        #log.writeLog("login...")

        session=""

        try:
            url=self.baseApiUrl+operationUrl
            print(url)
            logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": url})
            payload = json.dumps({
                "username": self.user,
                "password": self.password
            })
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            response = requests.request("POST",url , headers=headers, data=payload)
            # print(response.text)
            session = json.loads(response.text)["userInfo"]["sessionId"]
            print("end login")
            logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": "end login"})
            #log.writeLog("end login")
        except Exception as e:
            print(e.__traceback__)
            logger.error({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data":  e.__traceback__})
            #log.writeLog(e.__traceback__)

        return session



    def postRequest(self,requestUrl,payload,methodType,responseType):

        try:

            print("Start: {}{}".format(requestUrl,'...'))
            logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data":  "Start: {}{}".format(requestUrl,'...')})
            #log.writeLog("Start: {}{}".format(requestUrl,'...'))

            headers = {
                'Accept': 'application/json',
                'INFA-SESSION-ID': self.sessionID,
                'Content-Type': 'application/json'

            }
            response = requests.request( methodType, requestUrl, headers=headers, data=payload)
            if response.status_code!=200:
                print(response.status_code)

                logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI",
                             "data": response.status_code  })


                return {"status":response.status_code,"body":response.reason}
            else:
                if responseType=="dict":
                    dictresult = json.loads(response.text)

                    print("End: {}".format(requestUrl))
                    logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI",
                                 "data":  "End: {}".format(requestUrl)})
                    #log.writeLog("End: {}".format(requestUrl))
                    return {"status": response.status_code, "body": dictresult}
                else:
                    return  {"status": response.status_code, "body": response.content}

        except Exception as e:
            print(e.__traceback__)
            logger.error({"date": str(timestampNow), "source": "MAIN-IICSAPI",
                         "data":  e.__traceback__})
            #log.writeLog("e.__traceback__")
            return   {"status": 401, "body": e.__traceback__}




    def getAssets(self, operationUrl, fromDate,type):
        try:
            print(f'GetAssets from:',{fromDate})

            logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI",
                         "data": "GetAssets from:{}".format(fromDate) })

            #log.writeLog(f'GetAssets from:{fromDf'GetAssets from:',{fromDate}ate}')
            url="{}{}?q=updateTime>={} and type=='{}'".format(self.baseApiUrl,operationUrl,fromDate,type)
            #url = self.baseApiUrl+ operationUrl+ "?" + "updateTime>=" + fromDate +" and type=="+'map' "
            print(url)
            logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": url })

            #log.writeLog(url)
            dictResult=self.postRequest(url,"","GET","dict")
            if(dictResult["status"]==200 ):
                print(f'Number of Assets:{dictResult["body"]["count"]}')
                txt=f'Number of Assets:{dictResult["body"]["count"]}'
                logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": txt})
                #log.writeLog(f"Number of Assets:", {dictResult["body"]["count"]})
                return dictResult["body"]



        except Exception as e:
            print(e.__traceback__)
            logger.error({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": e.__traceback__})
            return None



    def getExportId(self,assetId,asseType,assetPath,operationUrl,dt_string):

        url = self.baseApiUrl + operationUrl
        print(url)
        assetName = assetPath.split("/")[-1]
        try:
            payload = json.dumps({
                "name": assetId + '_' + assetName + '_' + asseType + '_' + dt_string,
                "objects": [
                    {
                        "id": assetId,
                        "includeDependencies": True
                    }
                ]
            })

            dict=self.postRequest(url,payload,"POST","dict")
            if(dict["status"]==200):
                print(dict["body"]["status"])
                print("end ExportAssetJson")

                logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data":"end ExportAssetJson"})

                #log.writeLog(dict["body"]["status"])
                #log.writeLog("end ExportAssetJson")


                return dict["body"]["id"]
            else:
                print(dict["body"])
                #log.writeLog(dict["body"])
                return 401
        except Exception as e:
            print(e.__traceback__)
            logger.error({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": e.__traceback__})
            #log.writeLog( e.__traceback__)
            return None



    def getExportStatus(self, exportId, operationUrl):

        url = self.baseApiUrl + operationUrl
        print(url)
        #log.writeLog(url)

        try:
            payload = ""

            dict = self.postRequest(url, payload, "GET", "dict")
            if (dict["status"] == 200):
                print(dict["body"]["status"]["state"])
                print("end ExportAssetJson")
                logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": "end ExportAssetJson"})
                #log.writeLog(dict["body"]["status"]["state"])
                #log.writeLog("end ExportAssetJson")
                return dict["body"]["status"]["state"]
            else:
                print(dict["body"])
                #log.writeLog( dict["body"])
                return 401
        except Exception as e:
            print(e.__traceback__)

            logger.error({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": e.__traceback__})
            #log.writeLog( e.__traceback__)
            return None

    def exportAssetJson(self,operationUrl,assetId,asseType,assetPath):
        print("ExportAssetJson...")
        logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": "ExportAssetJson..."})

        #log.writeLog( "ExportAssetJson...")
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

        jobID= self.getExportId(assetId,asseType,assetPath, operationUrl,dt_string)
        assetname=assetPath.split("/")[-1]
        #filename= "{}_{}_{}.zip".format(assetname,assetId)
        filename = "{}^{}^{}.zip".format(assetId,assetname,asseType )
        if jobID!=401:
            status=self.getExportStatus(jobID, operationUrl + "/" + jobID)
            while status!="SUCCESSFUL":
                   status = self.getExportStatus(jobID, operationUrl + "/" + jobID)



            url="{}{}{}{}/package".format(self.baseApiUrl,operationUrl,"/",jobID)
            stream= self.postRequest(url,"","GET","stream")
            if (not os.path.isdir('{}rawData'.format(WORKINGDIR))):
               try:
                pathlib.Path(WORKINGDIR+'rawData').mkdir(parents=True, exist_ok=True)
               except Exception as e:
                   print( "error creating dir")
                   logger.error({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": e.__traceback__})
                   #log.writeLog("error creating dir")
            filename='{}rawData/{}'.format(WORKINGDIR,filename)
            with open(filename, 'wb') as f:
                print("create file:",filename)
                logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": "create file:{}".format(filename)})
                if(stream["status"]==200):
                    f.write(stream['body'])
                    time.sleep(5)

    def exportRawJsonFromZip(self,workingFolder,assetID, obj):

       for file in os.listdir(workingFolder):
          print("start unzipping...")
          logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": "start unzipping..."})

          try:
               if (file.endswith(".zip")):
                   assetId,assetName,assetType= file.replace(".zip","").split("^")
                   if (file.endswith(".zip") and  assetId==assetID):
                       Utility.unzipFile(workingFolder + "/" + file, workingFolder + '/' + assetName)

                       for f in os.listdir(workingFolder+'/'+assetName+"/Explore"):
                           if os.path.isdir( workingFolder+'/'+assetName+"/Explore/"+f):
                                projectName=f
                                for final in  os.listdir( workingFolder+'/'+assetName+"/Explore/"+f):
                                    if(final.endswith(".zip")):
                                        Utility.unzipFile(workingFolder + '/' + assetName + "/Explore/" + f + "/" + final, workingFolder + '/' + assetName + "/Explore/" + f + "/" + assetName)
                                        for filename in os.listdir( workingFolder+'/'+assetName+"/Explore/"+f+"/"+assetName+"/bin"):
                                            if filename=="@3.bin":
                                                Utility.copyFile(workingFolder + '/' + assetName + "/Explore/" + f + "/" + assetName + "/bin/@3.bin", workingFolder + '/' + assetName + '.json')
                                                with open(workingFolder+'/'+assetName+'.json',"r") as mFile:
                                                    mText= mFile.readline()
                                                    mConnections= self.exportRawConnJsonFromZip(workingFolder+"/"+assetName)
                                                    newDoc={}
                                                    newDoc["connections"]=mConnections
                                                    newDoc["mapName"]=assetName
                                                    newDoc["map"]=json.loads(mText)
                                                    newDoc["ProjectName"]=projectName
                                                    newDoc["id"]=obj["id"]
                                                    newDoc["path"]=obj["path"]
                                                    newDoc["description"]=obj["description"]
                                                    newDoc["updatedBy"] = obj["updatedBy"]
                                                    newDoc["updateTime"] = obj["updateTime"]
                                                    newDoc["tags"] = obj["tags"]
                                                    newDoc["sourceControl"] = obj["sourceControl"]
                                                    newDoc["customAttributes"] = obj["customAttributes"]
                                                    with open(workingFolder + '/' + assetName + '.json',"w") as mFileOut:
                                                            mFileOut.write(json.dumps(newDoc))
          except Exception as e:
           print(e.__traceback__)
           logger.error({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": e.__traceback__})







                                                    #upload file to HDFS
                                                       #push content of file to Kafka
                                                       #push file to Mongo for sace of completions




        # get name,id and type out of the name of the file
        #unzip- take :"Explore"
        #In "Explore" you get 2 items : 1)Folder and json - take the folder
        #in the folder  - you got another zip- unzip it
        #go to bin folder - the json is @3.bin

    def exportRawConnJsonFromZip(self,workingFolder):
       listOfConnections =[]

       try:
           for conn in os.listdir(workingFolder+"/SYS"):
               try:
                   if conn.endswith("Connection.zip"):
                       connWithoutZip=conn.replace(".zip","")
                       Utility.unzipFile(workingFolder + '/' + "/SYS/" + conn,
                                         workingFolder + '/' + "/SYS/" + connWithoutZip)
                       for final in os.listdir(workingFolder+'/'+"/SYS/"+connWithoutZip):
                           if final=="connection.json":
                               with open(workingFolder+'/'+"/SYS/"+connWithoutZip+"/connection.json","r") as fConn:
                                connText=fConn.readline()
                                listOfConnections.append(json.loads(connText))
               except Exception as e:
                   print(e.__traceback__)
                   logger.error({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": e.__traceback__})
       except Exception as e:
           print(e.__traceback__)
           logger.error({"date": str(timestampNow), "source": "MAIN-IICSAPI", "data": e.__traceback__})

       return  listOfConnections




        # get name,id and type out of the name of the file
        #unzip- take :"Explore"
        #In "Explore" you get 2 items : 1)Folder and json - take the folder
        #in the folder  - you got another zip- unzip it
        #go to bin folder - the json is @3.bin



class Program:
    def __init__(self,conf):
        self.conf=conf

    def main(self,fromDate):
        #baseApiUrl = "https://usw5.dm-us.informaticacloud.com/saas"
        baseApiUrl=self.conf.data["baseApiUrl"]
        #loginUrl="/public/core/v3/login"
        loginUrl=self.conf.data["loginUrl"]
        #userName='zacay.daushin1@gmail.com'
        userName = self.conf.data["userName"]
        #password="Nadav2009"
        password = self.conf.data["password"]
        session=IICSApi(userName,password,baseApiUrl,loginUrl)
        assetsDict=session.getAssets("/public/core/v3/objects", fromDate,"DTEMPLATE")
        assetDictObj=None
        if  assetsDict is not None:
            assetDictObj= assetsDict["objects"]

        i=1
        if  bool(assetDictObj):
            for obj in assetDictObj:
                 if(obj["type"]=="DTEMPLATE"):
                     session.exportAssetJson("/public/core/v3/export", obj["id"], obj["type"], obj["path"])
                     print("Asset number:{} out {} ".format(i,len(assetDictObj)))
                     logger.info({"date": str(timestampNow), "source": "MAIN-IICSAPI",
                                  "data": "Asset number:{} out {} ".format(i,len(assetDictObj))  })
                     i = i + 1
                     assetname = obj["path"].split("/")[-1]


                 session.exportRawJsonFromZip("{}rawData".format(WORKINGDIR),obj["id"], obj)



#
# pg = Program()
# pg.main('1900-11-21T12:00.00Z')

class Run:

    def run(self):
        conf=ConfigurationFile()
        WORKINGDIR=conf.data["WORKINGDIR"]

        while True:
            assetName = "lastRun"
            if 'lastRun.json' not in os.listdir(WORKINGDIR):
                lastRunD = {"lastRun": "1900-03-01T12:00:00Z"}
                with open(WORKINGDIR + '/' + assetName + '.json', "w") as jsonFile:
                    json.dump(lastRunD, jsonFile)
                    jsonFile.close()
            else:

                with open(WORKINGDIR + '/' + assetName + '.json', "r") as jsonFile:
                    txt = jsonFile.readlines()
                    lastRunD = json.loads(txt[0])
                pg = Program(conf)
                current_time = datetime.now()
                pg.main(lastRunD["lastRun"])
                lastRunD["lastRun"] = current_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                with open(WORKINGDIR + '/' + assetName + '.json', "w") as jsonFile:
                    json.dump(lastRunD, jsonFile)
                    jsonFile.close()

            time.sleep(5)

r=Run()
r.run()








