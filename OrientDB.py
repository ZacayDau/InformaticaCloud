import hashlib
import logging
import time
from datetime import datetime
from os.path import exists

import requests
import json


import spark as sparisGuidExistsOnOrientDBk
from kafka import KafkaConsumer
from pyparsing import col
from pyspark.python.pyspark.shell import sc
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat

from ConfigurationFile import ConfigurationFile
from Slack import Slack
from os.path import exists


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


class OrientDB:

# 'Authorization': 'Basic cm9vdDpyb290'
    def __init__(self, basicAuth, url, databasename,port,mapName):
        self.basicAuth = basicAuth
        self.url = url
        self.databasename = databasename
        self.port=port
        self.mapName=mapName

        cn = ConfigurationFile()
        dictPath=cn.data["mapDictPath"]+"VERTEX"+".json"
        dictPathE = cn.data["mapDictPath"] + "EDGE.json"
        self.dictPath=dictPath
        self.dictPathE=dictPathE
        if exists(dictPath):
            with open(dictPath) as f:
                lines = f.readlines()
                self.mapDict=json.loads(lines[0])
        else:
            self.mapDict={}

        if exists(dictPathE):
            with open(dictPathE) as f:
                lines = f.readlines()
                self.mapDictE=json.loads(lines[0])
        else:
            self.mapDictE={}

    def isDbExists(self):
        url = "{}:{}/database/{}".format(self.url, self.port, self.databasename)
        print(url)
        logger.info({"date": str(timestampNow), "source": "OrientDB", "data": url})

        logger.info({"date": str(timestampNow), "source": "OrientDB", "data": "login..."})

        payload = {}
        headers = {
            'Authorization': self.basicAuth
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        print(response.text)
        dictResult = json.loads(response.text)
        if "errors" not in dictResult:
            return True
        else:
            return False

    def createClass(self,ObjectType):

        if  ObjectType=="V":
            command="CREATE CLASS COLUMN EXTENDS V"
            self.runCommand(command)
        else:
            command="CREATE CLASS DATAFLOW EXTENDS E"
            self.runCommand(command)

    def createDatabase(self):

        if (not self.isDbExists()):
                url="{}:{}/database/{}/plocal".format(self.url,self.port,self.databasename)
                payload = {}
                headers = {
                    'Authorization': self.basicAuth
                }
                print(url)
                logger.info({"date": str(timestampNow), "source": "OrientDB", "data": url})
                response = requests.request("POST", url, headers=headers, data=payload)

                dictResult= json.loads(response.text)
                if "errors" not in dictResult:
                    print("create class Column..")
                    logger.info({"date": str(timestampNow), "source": "OrientDB", "data":  "create class Column.."})

                    self.createClass("V")
                    print("create class Edge..")
                    logger.info(
                        {"date": str(timestampNow), "source": "OrientDB", "data": "create class Column.."})

                    self.createClass("E")
                    print("Database:{} created successfully".format(self.databasename))
                    logger.info({"date": str(timestampNow), "source": "OrientDB", "data": "Database:{} created successfully".format(self.databasename)})
                else:
                    print("errors creating database {} ".format( self.databasename))
                    logger.error({"date": str(timestampNow), "source": "OrientDB",
                                 "data":  "errors creating database {} ".format( self.databasename)})
        else:
            print("Database:{} already exists!".format( self.databasename))
            logger.info({"date": str(timestampNow), "source": "OrientDB", "data": "Database:{} already exists!".format( self.databasename)})

    def runCommand(self,command):
       url = "{}:{}/command/{}/sql".format(self.url, self.port, self.databasename)
       payload = json.dumps({
                "command": command
            })
       headers = {
                'Authorization': self.basicAuth,
                'Content-Type': 'application/json'
            }
       print(command)
       logger.info({"date": str(timestampNow), "source": "OrientDB", "data":  command})
       response = requests.request("POST", url, headers=headers, data=payload)
       time.sleep(2)
       print(response.text)
       logger.info({"date": str(timestampNow), "source": "OrientDB", "data": response.text})
       return  json.loads(response.text)

    def createVertexes(self,df):
        dictCommandData={}
        dictCommandMap={}
        i=0
        for row in df.collect():

            keys=["ContainerObjectName",
                  "ContainerObjectPath",
                  "ContainerObjectType",
                  "ContainerToolType",
                  "ToolType",
                  "ControlflowName",
                  "ControlflowPath",
                  "DatabaseName",
                  "ServerName",
                  "DataType",
                  "IsObjectData",
                  "LayerName",
                  "ColumnDataGuid",
                  "ColumnDataId",
                  "ColumnMapGuid",
                  "ColumnMapId",
                  "ColumnName",
                  "ObjectType",
                  "Provider",
                  "Precision" ,
                  "Scale",
                  "SchemaName",
                  "TableName"


                 ]
            for k in keys:
                 dictCommandData[k]= row[k]
                 dictCommandMap[k]=row[k]

            dictCommandData["ObjectGUID"]=dictCommandData["ColumnDataGuid"]
            dictCommandData["ObjectID"]=dictCommandData["ColumnDataId"]
            dictCommandData.pop('ColumnDataGuid', None)
            dictCommandData.pop('ColumnDataId', None)
            dictCommandData.pop('ColumnMapGuid', None)
            dictCommandData.pop('ColumnMapId', None)
            dictCommandData["IsMap"]="0"
            dictCommandData["IsVisible"]="1"

            #renames:
            dictCommandData["ConnectionID"] = "100"
            dictCommandData["ConnLogicName"]="InformaticaCloud"
            dictCommandData["ControlFlowName"] = dictCommandData["ControlflowName"]
            dictCommandData["ControlFlowPath"] = dictCommandData["ControlflowPath"]
            dictCommandData.pop('ControlflowName', None)
            dictCommandData.pop('ControlflowPath', None)
            dictCommandData["DisplayConnectionId"]="100"
            dictCommandData["ObjectName"]=dictCommandData["ColumnName"]
            dictCommandData.pop('ColumnName',None)
            dictCommandData["ToolName"]=dictCommandData["ContainerToolType"]
            dictCommandData["UpdatedDate"]= datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

            # renames:
            dictCommandMap["ConnectionID"] = "100"
            dictCommandMap["ConnLogicName"] = "InformaticaCloud"
            dictCommandMap["ControlFlowName"] = dictCommandMap["ControlflowName"]
            dictCommandMap["ControlFlowPath"] = dictCommandMap["ControlflowPath"]
            dictCommandMap.pop('ControlflowName', None)
            dictCommandMap.pop('ControlflowName', None)
            dictCommandMap["DisplayConnectionId"] = "100"
            dictCommandMap["ObjectName"] = dictCommandMap["ColumnName"]
            dictCommandMap.pop('ColumnName', None)
            dictCommandMap["ToolName"] = dictCommandMap["ContainerToolType"]
            dictCommandMap["UpdatedDate"] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")



            commadColumnData="CREATE VERTEX COLUMN CONTENT "+ json.dumps(dictCommandData)



            dictCommandMap["ObjectGUID"]=dictCommandMap["ColumnMapGuid"]
            dictCommandMap["ObjectID"]=dictCommandMap["ColumnMapId"]
            dictCommandMap.pop('ColumnDataGuid', None  )
            dictCommandMap.pop('ColumnDataId', None)
            dictCommandMap.pop('ColumnMapGuid', None)
            dictCommandMap.pop('ColumnMapId', None)
            dictCommandMap["IsObjectData"]="0"
            dictCommandMap["IsMap"]="1"

            if (row["IsObjectData"])=="1":
                dictCommandMap["IsVisible"]="1"
            else:
                 dictCommandMap["IsVisible"]="0"

            commadColumnMap="CREATE VERTEX COLUMN CONTENT "+ json.dumps(dictCommandMap)



            if (row["IsObjectData"])=="1":

                print("Object is: "+dictCommandData["ObjectGUID"])
                logger.info({"date": str(timestampNow), "source": "OrientDB", "data":"Object is: "+dictCommandData["ObjectGUID"] })
                if  not self.isGuidExistsOnOrientDB(dictCommandData["ObjectGUID"]) :
                    print("create v1 {}".format(str(i)))
                    logger.info({"date": str(timestampNow), "source": "OrientDB", "data": "create v1 {}".format(str(i))})
                    res=self.runCommand(commadColumnData)
                    self.mapDict[dictCommandData["ObjectGUID"]]=res["result"][0]["@rid"]


                else:
                    rid=self.getRidbyObjectGuid(dictCommandData["ObjectGUID"])
                    #{k: v for k, v in points.items() if v[0] < 5 and v[1] < 5}
                    vals={k:v for k,v in dictCommandData.items() if k in ["Precision","Scale","DataType"]}
                    stmt=self.generateUpdateCommand(vals,rid)


                print("Object is: " + dictCommandMap["ObjectGUID"])
                logger.info({"date": str(timestampNow), "source": "OrientDB", "data":"Object is: " + dictCommandMap["ObjectGUID"] })
                if  not self.isGuidExistsOnOrientDB(dictCommandMap["ObjectGUID"]) :
                     print("create v2 {}".format(str(i)))
                     logger.info({"date": str(timestampNow), "source": "OrientDB",
                                  "data":  "create v2 {}".format(str(i))})
                     res=self.runCommand(commadColumnMap)
                     self.mapDict[dictCommandMap["ObjectGUID"]] = res["result"][0]["@rid"]

            else:

                if  not self.isGuidExistsOnOrientDB(dictCommandMap["ObjectGUID"]) :
                 print("create v3 {}".format(str(i)))
                 logger.info({"date": str(timestampNow), "source": "OrientDB",
                              "data": "create v3 {}".format(str(i))})
                 res=self.runCommand(commadColumnMap)
                 self.mapDict[commadColumnMap["ObjectGUID"]] = res["result"][0]["@rid"]


                else:
                    rid = self.getRidbyObjectGuid(dictCommandMap["ObjectGUID"])
                    # {k: v for k, v in points.items() if v[0] < 5 and v[1] < 5}
                    vals = {k: v for k, v in dictCommandMap.items() if k in ["Precision", "Scale", "DataType"]}
                    stmt = self.generateUpdateCommand(vals, rid)
            i=i+1
        self.saveJsonToFile(self.dictPath,self.mapDict)
        # jsonTxt=json.dumps(self.mapDict)
        # print(jsonTxt)
        # f = open(self.dictPath , "w")
        # f.write(jsonTxt)


    def saveJsonToFile(self,dictPath,dictM):
        if not exists(dictPath):
            jsonTxt = json.dumps(dictM)
            f = open(dictPath, "w")
            f.write(jsonTxt)
        else:
            with open(dictPath, "r+") as file:
                data = json.load(file)
                data.update(dictM)
                file.seek(0)
                json.dump(data, file)

    def isGuidExistsOnOrientDB(self,guid):
        if guid in self.mapDict:
            return True
        else:
            return False

        # query="select ObjectGUID,@rid from COLUMN where ObjectGUID='{}' ".format(guid)
        # dicresult=self.runCommand(query)
        # print(dicresult["result"])
        # print(len(dicresult["result"] ))
        #
        # if len(dicresult["result"] )==0:
        #     return False
        # else:
        #     return True


    def isEdgeExist(self,fromrid,torid):

        if fromrid+"-"+torid in self.mapDictE:
            return  True
        else:
            return False
        # query="select @rid from DATAFLOW where in='{}' and out='{}' ".format(toGuid,fromGuid)
        # dicresult=self.runCommand(query)
        # print(dicresult["result"])
        # print(len(dicresult["result"] ))
        #
        # if len(dicresult["result"] )==0:
        #     return False
        # else:
        #     return True


    def getRidbyObjectGuid(self, guid):
       if guid in self.mapDict:
            return self.mapDict[guid]
       else:
           return "-1"
        # query = "select @rid from COLUMN where ObjectGUID='{}' ".format(guid)
        # dicresult = self.runCommand(query)
        # rid = "0"
        #
        # if len(dicresult["result"]) > 0:
        #     rid = dicresult["result"][0]["@rid"]
        # else:
        #     rid = "-1"
        # return rid





    def createEdges(self,df):
        for row in df.collect():
            if( row["SourceIsObjectData"]=="1" and  row["TargetIsObjectData"]=="1") :
                print("case1 -3 edges t->M->M->T")
                sguid1=row["SourceColumnDataGuid"]
                srid1=self.getRidbyObjectGuid(sguid1)
                sguid2=row["SourceColumnMapGuid"]
                srid2=self.getRidbyObjectGuid(sguid2)
                tguid3=row["TargetColumnMapGuid"]
                trid3=self.getRidbyObjectGuid(tguid3)
                tguid4=row["TargetColumnDataGuid"]
                trid4=self.getRidbyObjectGuid(tguid4)

                if (not self.isEdgeExist(srid1,srid2)):
                    cmd="CREATE EDGE DATAFLOW FROM {} TO {}".format(srid1,srid2)
                    print("From:{} to: {}".format(sguid1,sguid2))
                    logger.info({"date": str(timestampNow), "source": "OrientDB",
                                 "data":  "From:{} to: {}".format(sguid1,sguid2) })
                    res=self.runCommand(cmd)
                    self.mapDictE[srid1+"-"+ srid2 ] = res["result"][0]["@rid"]

                if (not self.isEdgeExist(srid2,trid3)):
                    cmd="CREATE EDGE DATAFLOW FROM {} TO {}".format(srid2,trid3)
                    print("From:{} to: {}".format(sguid2, tguid3))
                    logger.info({"date": str(timestampNow), "source": "OrientDB",
                                 "data": "From:{} to: {}".format(sguid2, tguid3)})
                    res=self.runCommand(cmd)
                    self.mapDictE[srid2 + "-" + trid3] = res["result"][0]["@rid"]

                if (not self.isEdgeExist(trid3,trid4)):
                    cmd="CREATE EDGE DATAFLOW FROM {} TO {}".format(trid3,trid4)
                    print("From:{} to: {}".format(tguid3, tguid4))

                    logger.info({"date": str(timestampNow), "source": "OrientDB",
                                 "data": "From:{} to: {}".format(tguid3, tguid4)})

                    res=self.runCommand(cmd)
                    self.mapDictE[trid3 + "-" + trid4] = res["result"][0]["@rid"]



            elif(row["SourceIsObjectData"]=="1" and  row["TargetIsObjectData"]=="0"):
                 print("case2 -2 edges t->M->M")
                 sguid1=row["SourceColumnDataGuid"]
                 srid1=self.getRidbyObjectGuid(sguid1)
                 sguid2=row["SourceColumnMapGuid"]
                 srid2=self.getRidbyObjectGuid(sguid2)
                 tguid3=row["TargetColumnMapGuid"]
                 trid3=self.getRidbyObjectGuid(tguid3)


                 if (not self.isEdgeExist(srid1,srid2)):
                    cmd="CREATE EDGE DATAFLOW FROM {} TO {}".format(srid1,srid2)
                    print("From:{} to: {}".format(sguid1, sguid2))
                    logger.info({"date": str(timestampNow), "source": "OrientDB",
                                 "data": "From:{} to: {}".format(sguid1, sguid2)})

                    res = self.runCommand(cmd)
                    self.mapDictE[srid1 + "-" + srid2] = res["result"][0]["@rid"]



                 if (not self.isEdgeExist(srid2,trid3)):
                    cmd="CREATE EDGE DATAFLOW FROM {} TO {}".format(srid2,trid3)
                    print("From:{} to: {}".format(sguid2, sguid2))

                    logger.info({"date": str(timestampNow), "source": "OrientDB",
                                 "data": "From:{} to: {}".format(sguid2, sguid2)})

                    res = self.runCommand(cmd)
                    self.mapDictE[srid2 + "-" + trid3] = res["result"][0]["@rid"]


            elif (row["SourceIsObjectData"]=="0" and  row["TargetIsObjectData"]=="1"):
                 print("case3 -2 edges M->M->T")

                 logger.info({"date": str(timestampNow), "source": "OrientDB",
                              "data":  "case3 -2 edges M->M->T"})


                 sguid1=row["SourceColumnMapGuid"]
                 srid1=self.getRidbyObjectGuid(sguid1)

                 tguid2=row["TargetColumnMapGuid"]
                 trid2=self.getRidbyObjectGuid(tguid2)
                 tguid3=row["TargetColumnDataGuid"]
                 trid3=self.getRidbyObjectGuid(tguid3)
                 if (not self.isEdgeExist(srid1, trid2)):
                     cmd="CREATE EDGE DATAFLOW FROM {} TO {}".format(srid1,trid2)
                     print("From:{} to: {}".format(sguid1, tguid2))
                     logger.info({"date": str(timestampNow), "source": "OrientDB",
                                  "data": "From:{} to: {}".format(sguid1, tguid2) })
                     res = self.runCommand(cmd)
                     self.mapDictE[srid1 + "-" + trid2] = res["result"][0]["@rid"]

                 if (not self.isEdgeExist(trid2, trid3)):
                     cmd="CREATE EDGE DATAFLOW FROM {} TO {}".format(trid2,trid3)
                     print("From:{} to: {}".format(tguid2, tguid3))
                     logger.info({"date": str(timestampNow), "source": "OrientDB",
                                  "data": "From:{} to: {}".format(tguid2, tguid3)})
                     res = self.runCommand(cmd)
                     self.mapDictE[trid2 + "-" + trid3] = res["result"][0]["@rid"]


            elif (row["SourceIsObjectData"]=="0" and  row["TargetIsObjectData"]=="0"):
                 print("case4 -1 edges M->M")
                 sguid1=row["SourceColumnMapGuid"]
                 srid1=self.getRidbyObjectGuid(sguid1)
                 tguid2=row["TargetColumnMapGuid"]
                 trid2=self.getRidbyObjectGuid(tguid2)

                 if (not self.isEdgeExist(srid1,trid2)):
                    cmd="CREATE EDGE DATAFLOW FROM {} TO {}".format(srid1,trid2)
                    print("From:{} to: {}".format(sguid1, tguid2))

                    logger.info({"date": str(timestampNow), "source": "OrientDB",
                                 "data": "From:{} to: {}".format(sguid1, tguid2)})

                    res = self.runCommand(cmd)
                    self.mapDictE[srid1 + "-" + trid2] = res["result"][0]["@rid"]

        # jsonTxt = json.dumps(self.mapDictE)
        # print(jsonTxt)
        # f = open(self.dictPathE, "w")
        # f.write(jsonTxt)
        self.saveJsonToFile(self.dictPathE,self.mapDictE)

    def checkAnomaly(self,mapName):

        cn = ConfigurationFile()
        url=cn.data["urlSlack"]
        #url = "https://hooks.slack.com/services/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"


        try:
            query = "select DataType,MapName,LayerName,ColumnName from (MATCH {class:COLUMN,where:(ControlFlowPath=\'{0}\'), " \
                    "as: a, maxDepth:15, pathAlias:qwr}.out('DATAFLOW')" \
                    "{class:COLUMN, as: b,maxDepth:15, pathAlias:qwr} " \
                    "RETURN DISTINCT b.@rid as final, qwr[IsVisible = 1] as Path,qwr.DataType as DataType " \
                    ",b.out().size() as size,a.ControlFlowPath as map,qwr.ControlFlowPath as MapName," \
                    "qwr.LayerName as  LayerName,qwr.ObjectName as ColumnName) where size =1"
            query = query.replace("{0}", str(mapName))
            myDict=self.runCommand(query)

            #x=[ len(set(dictT.values())) for dictT in  myDict["result"]]
            setOfDataTypes=[ s for s in myDict["result"] if len(set(s["DataType"]))>1 ]
            if len(setOfDataTypes)>0:
                dtypes=[dictt["DataType"] for dictt in setOfDataTypes]
                maps = [dictt["MapName"] for dictt in setOfDataTypes]
                layers = [dictt["LayerName"] for dictt in setOfDataTypes]
                columns = [dictt["ColumnName"] for dictt in setOfDataTypes]

                maxLen = max([len(item) for item in maps])
                dtypes=[ x for x   in dtypes if len(x)==maxLen][0]
                maps = [x for x in maps if len(x) == maxLen][0]
                layers = [x for x in layers if len(x) == maxLen][0]
                columns = [x for x in columns if len(x) == maxLen][0]




                i=0
                txt=""

                for item in maps:
                    txt=txt+"map:{}.layer:{}.column:{}.datatype:{}=>".format(item,layers[i],columns[i],dtypes[i])
                    i=i+1

                ls= [ x for x in txt.split("=>") if x!=""]

                message="There is a mismatch of DataType on map:{}".format(mapName)
                message = message +"\n"+"path:" + "\n"+\
                          "=>\n".join(ls)
                slackClient=Slack(url)
                print(message)

                logger.info({"date": str(timestampNow), "source": "OrientDB",
                             "data":  message})

                slackClient.sendMessage(message)
        except Exception as e:
            print(e)
            logger.error({"date": str(timestampNow), "source": "OrientDB", "data": e.__traceback__})

    def generateUpdateCommand(self,myDict,rid):


        foodict = {k.replace("'",""): v for k, v in myDict.items() }
        cmd="Update {} set ".format(rid)
        cm=""
        i=0
        for x in foodict:
            if(i==len(foodict)-1):
                cmd1=x.replace("'","")+"="+ str(foodict[x])
            else:
                cmd1 = x.replace("'", "") + "=" + str(foodict[x])+","
            cm=cm+cmd1
            i=i+1

        print(cmd+cm)
        logger.info({"date": str(timestampNow), "source": "OrientDB", "data": cmd+cm})

        return cmd

#orientClient = OrientDB("Basic cm9vdDpyb290", "http://localhost", "demodb", "2480","LoadtoSTRG")
#print(orientClient.isDbExists())

#orientClient.checkAnomaly(mapName)