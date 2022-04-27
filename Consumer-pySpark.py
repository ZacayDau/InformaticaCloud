# !pip install TextBlob
import json
import os

import hashlib
import spark as spark
from kafka import KafkaConsumer
from pyparsing import col
from pyspark.python.pyspark.shell import sc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, expr, when, current_timestamp, concat, udf, md5, \
    unix_timestamp
from pyspark.sql.types import StringType

from ConfigurationFile import ConfigurationFile


# conection between  spark and kafka
from OrientDB import OrientDB
from flatjson import Autoflatten

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

cn=ConfigurationFile()
# Set the consumer

#bootstrapServers = "cnt7-naya-cdh63:9092"
bootstrapServers=cn.data["brokers"]
#topics = "Informatica-Data"
topics=cn.data["topic"]


#BASIC_AUTH = "Basic cm9vdDpyb290"
BASIC_AUTH=cn.data["BASIC_AUTH"]
#HTTP_URL = "http://localhost"
HTTP_URL = cn.data["HTTP_URL"]

#HTTP_URL="http://stage2-prod.orient.stage.octopai-corp.local"
#DATABASE_NAME = "Customer_E2E"
DATABASE_NAME=cn.data["DATABASE_NAME"]
#PORT = "2480"
PORT=cn.data["PORT"]



spark = SparkSession\
        .builder\
        .appName("Read-Informatica-Data")\
        .getOrCreate()


class ConsumerPySpark:
    def __init__(self, bootstrap_servers, topics):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.consumer= consumer = KafkaConsumer(
            self.topics,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000)


    def flatRawMap(self):
        for message in self.consumer:

            events = json.loads(message.value)
            events = json.loads(events)
            jsonStr=json.dumps(events)
            jsonStr=jsonStr.replace("$$","").replace("##","")


            tsRDD=sc.parallelize([jsonStr])
            dfRaw=spark.read.option("multiline","true").json(tsRDD)
            #df2.show(10,False)
            #df2.printSchema()
            cf=Autoflatten(dfRaw)
            dfflat= cf.flattenX()
            #dfflat.show(1)
            print("links:")
            dflinks = self.getlinks(dfflat)
            #dflinks.show(3)
            print("connections:")
            dfConns=self.getDfConnections(dfflat)
            #dfConns.show(1)
            print("transformations:")
            dfTrans=self.getTransformations(dfflat)
            #dfTrans.show(3)
            print("transformationsDataObject:")
            dtTransDataObjects=self.getTransformationsDataObject(dfflat)








            dtTansColumnsData = self.getTansfomrationColumnsData(dfflat)
            print("data columns:")


            dtTansColumns = self.getTansfomrationColumns(dfflat)
            print("columns:")
           # dtTansColumns.show(1)

            manualMappingDF = self.createManualMappingDF(dtTransDataObjects)
            colsDF=self.createDfColumns(dtTansColumns)




            dtExp= dtTansColumns.filter(dtTansColumns.transformations_name=="Expression")
            #dtExp.show()
            dtexpinner= self.createInnerlinkExpression(dtExp)
            #dtexpinner.show()

            #dtTansColumns.filter(dtTansColumns.transformations_name=="Expression").show(1)
            #(df.state == "OH") & (df.gender == "M")



            # dtTansColumns.sort("transformations_name").filter( (dtTansColumns.transformations_name == "Sequence") ).show(
            #  60, False)

            #print("columns Data:")
            #dtTansColumnsData.sort("transformations_name").filter(dtTansColumnsData["transformations_name"]=="Sequence"). show(60, False)

            print("Df result:")
            df=self.createSourceToTarget(dflinks,
                                         dfConns,
                                         dfTrans,
                                         dtTansColumnsData,
                                         dtTansColumns,
                                         dtTransDataObjects,
                                         dtexpinner,
                                         manualMappingDF,
                                         colsDF)
            df=df.filter(df.TargetColumnName=="CustomerKey")
            df.printSchema()
            mapName = dflinks.select("mapName").rdd.flatMap(list).collect()
            print(mapName)
            #df.show(70)
            #
            cons.importDataframeToOrientDB(df, BASIC_AUTH, HTTP_URL, DATABASE_NAME, PORT)

            #mapName = dflinks.select("mapName").rdd.flatMap(list).collect()



            # orientClient = OrientDB(BASIC_AUTH, HTTP_URL, DATABASE_NAME, PORT,mapName)
            # orientClient.checkAnomaly(mapName)





    def importDataframeToOrientDB(self,df,basicAuth,url,database,port):
        dfSources = df.select(df["ContainerObjectName"],
                              df["ContainerObjectPath"],
                              df["ContainerObjectType"],
                              df["ContainerToolType"],
                              df["ToolType"],
                              df["ControlflowName"],
                              df["ControlflowPath"],
                              df["SourceId"],
                              df["SourceLayerName"],
                              df["SourceSchema"],
                              df["SourceDB"],
                              df["SourceServer"],
                              df["SourceProvider"],
                              df["SourceTable"],
                              df["SourceObjectType"],
                              df["SourceColumnName"],
                              df["SourceDataType"],
                              df["SourcePrecision"],
                              df["SourceScale"],
                              df["SourceIsObjectData"],
                              df["SourceColumnDataId"],
                              df["SourceColumnMapId"],
                              df["SourceColumnDataGuid"],
                              df["SourceColumnMapGuid"],
                              ).distinct() \
            .withColumnRenamed("SourceLayerName", "LayerName") \
            .withColumnRenamed("SourceSchema", "SchemaName") \
            .withColumnRenamed("SourceDB", "DatabaseName") \
            .withColumnRenamed("SourceServer", "ServerName") \
            .withColumnRenamed("SourceProvider", "Provider") \
            .withColumnRenamed("SourceTable", "TableName") \
            .withColumnRenamed("SourceObjectType", "ObjectType") \
            .withColumnRenamed("SourceColumnName", "ColumnName") \
            .withColumnRenamed("SourceDataType", "DataType") \
            .withColumnRenamed("SourcePrecision", "Precision") \
            .withColumnRenamed("SourceScale", "Scale") \
            .withColumnRenamed("SourceIsObjectData", "IsObjectData") \
            .withColumnRenamed("SourceColumnDataId", "ColumnDataId") \
            .withColumnRenamed("SourceColumnMapId", "ColumnMapId") \
            .withColumnRenamed("SourceColumnDataGuid", "ColumnDataGuid") \
            .withColumnRenamed("SourceColumnMapGuid", "ColumnMapGuid") \
            .withColumn("UpdatedDate", unix_timestamp(lit(current_timestamp()),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))

        dfTargets = df.select(df["ContainerObjectName"],
                              df["ContainerObjectPath"],
                              df["ContainerObjectType"],
                              df["ContainerToolType"],
                              df["ToolType"],
                              df["ControlflowName"],
                              df["ControlflowPath"],
                              df["TargetId"],
                              df["TargetLayerName"],
                              df["TargetSchema"],
                              df["TargetDB"],
                              df["TargetServer"],
                              df["TargetProvider"],
                              df["TargetTable"],
                              df["TargetObjectType"],
                              df["TargetColumnName"],
                              df["TargetDataType"],
                              df["TargetPrecision"],
                              df["TargetScale"],
                              df["TargetIsObjectData"],
                              df["TargetColumnDataId"],
                              df["TargetColumnMapId"],
                              df["TargetColumnDataGuid"],
                              df["TargetColumnMapGuid"],



                              ).distinct() \
            .withColumnRenamed("TargetLayerName", "LayerName") \
            .withColumnRenamed("TargetSchema", "SchemaName") \
            .withColumnRenamed("TargetDB", "DatabaseName") \
            .withColumnRenamed("TargetServer", "ServerName") \
            .withColumnRenamed("TargetProvider", "Provider") \
            .withColumnRenamed("TargetTable", "TableName") \
            .withColumnRenamed("TargetObjectType", "ObjectType") \
            .withColumnRenamed("TargetColumnName", "ColumnName") \
            .withColumnRenamed("TargetDataType", "DataType") \
            .withColumnRenamed("TargetPrecision", "Precision") \
            .withColumnRenamed("TargetScale", "Scale") \
            .withColumnRenamed("TargetIsObjectData", "IsObjectData") \
            .withColumnRenamed("TargetColumnDataId", "ColumnDataId")\
            .withColumnRenamed("TargetColumnMapId", "ColumnMapId") \
            .withColumnRenamed("TargetColumnDataGuid", "ColumnDataGuid") \
            .withColumnRenamed("TargetColumnMapGuid", "ColumnMapGuid") \
            .withColumn("UpdatedDate",unix_timestamp(lit(current_timestamp()), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))


        dfEdges=df.select(df["SourceColumnDataGuid"],
                          df["SourceColumnDataId"],
                          df["SourceColumnMapGuid"],
                          df["SourceColumnMapId"],
                          df["SourceIsObjectData"],

                          df["TargetColumnDataGuid"],
                          df["TargetColumnDataId"],
                          df["TargetColumnMapGuid"],
                          df["TargetColumnMapId"],
                          df["TargetIsObjectData"],
                          )
        # print("number of rows Sources:")
        # print(dfSources.show(1))
        #
        # print("number of rows Targets:")
        # print(dfTargets.show(1))
        #
        # print("number of rows Edges:")
        # print(dfEdges.show(1))

        #print("Edges:")
        # dfEdges.show(100)
        mapName = df.select("ControlflowPath").rdd.flatMap(list).collect()


        orientClient = OrientDB(basicAuth, url, database, port,mapName[0])
        orientClient.createDatabase()
        print("create vertex sources:")
        orientClient.createVertexes(dfSources)
        print("create vertex targets:")
        orientClient.createVertexes(dfTargets)
        print("create edges:")
        orientClient.createEdges(dfEdges)
        print("checkAnomaly:")
        orientClient.checkAnomaly(mapName[0])


    def createDfColumns(self,df):
        df = df.select(df["mapId"],
                       df["mapPath"],
                       df["transformations_ID"],
                       df["field_ID"],
                       df["column_Name"],
                       df["field_ID_Data"])
        return df


    def createManualMappingDF(self,df):

        df=df.filter(df.map_content_transformations_manualMappings_mappingList_toField_ID \
                .isNotNull())

        df=df.select(df["mapId"],
                     df["mapPath"],
                     df["transformations_ID"],
                     df["transformations_name"],
                     df["map_content_transformations_manualMappings_mappingList_fromFieldName"],
                     df["map_content_transformations_manualMappings_mappingList_toField_ID"]

                     )\
                     .withColumnRenamed("transformations_ID", "TargetId") \
                     .withColumnRenamed("transformations_name", "TargetLayerName") \
                     .withColumnRenamed("map_content_transformations_manualMappings_mappingList_fromFieldName", "fromFieldName") \
                      .withColumnRenamed("map_content_transformations_manualMappings_mappingList_toField_ID", "toField_ID")
        return df

    def createInnerlinkExpression(self,df):

        dfnew=df.withColumnRenamed("ProjectName","ContainerObjectName") \
        .withColumnRenamed("mapPath", "ContainerObjectPath") \
        .withColumnRenamed("mapName", "ControlflowName") \
        .withColumnRenamed("transformations_name", "SourceLayerName")

        dfnew=dfnew.withColumn("ControlflowPath", dfnew["ControlflowName"]) \
                .withColumn("SourceId", dfnew["transformations_ID"]) \
                .withColumn("SourceSchema", lit(""))\
                .withColumn("SourceDB", lit("")) \
                .withColumn("SourceServer", lit("")) \
                .withColumn("SourceProvider", lit("")) \
                .withColumn("SourceTable", lit("")) \
                .withColumn("SourceObjectType", lit("Expression")) \
                .withColumn("SourceSql", lit("")) \
                .withColumn("SourceConnectionKey", lit("")) \
                .withColumn("SourceColumnName", dfnew["expression"]) \
                .withColumn("SourceColumnID", dfnew["field_ID"]) \
                .withColumn("SourceDataType",   expr("case when  field_DataType like '%string%' then 'nvarchar'"\
                                                    "when field_DataType like '%integer%' then 'integer' "\
                                                    " else field_DataType end"))\
                .withColumn("SourcePrecision", dfnew["precision"]) \
                .withColumn("SourceScale", dfnew["scale"]) \
                .withColumn("TargetId", dfnew["transformations_ID"]) \
                .withColumn("TargetLayerName", dfnew["SourceLayerName"]) \
                .withColumn("TargetSchema", lit("")) \
                .withColumn("TargetDB", lit("")) \
                .withColumn("TargetServer", lit("")) \
                .withColumn("TargetProvider", lit("")) \
                .withColumn("TargetTable", lit("")) \
                .withColumn("TargetObjectType", lit("Expression")) \
                .withColumn("TargetSql", lit("")) \
                .withColumn("TargetConnectionKey", lit("")) \
                .withColumn("TargetColumnName", dfnew["column_Name"]) \
                .withColumn("TargetColumnID",dfnew["field_ID"] ) \
                .withColumn("TargetDataType", expr("case when  field_DataType like '%string%' then 'nvarchar'" \
                                                   "when field_DataType like '%integer%' then 'integer' " \
                                           " else field_DataType end")) \
            .withColumn("TargetPrecision", dfnew["precision"]) \
                .withColumn("TargetScale", dfnew["scale"])

        dfnew=dfnew.select( dfnew["ContainerObjectName"],
                                dfnew["ContainerObjectPath"],
                                 dfnew["ControlflowName"],
                                 dfnew["mapId"],
                                 dfnew["ControlflowPath"],
                                 dfnew["SourceId"],
                                 dfnew["SourceLayerName"],
                                 dfnew["SourceSchema"],
                                 dfnew["SourceDB"],
                                 dfnew["SourceServer"],
                                 dfnew["SourceProvider"],
                                 dfnew["SourceTable"],
                                 dfnew["SourceObjectType"],
                                 dfnew["SourceSql"],
                                 dfnew["SourceConnectionKey"],
                                 dfnew["SourceColumnName"],
                                 dfnew["SourceColumnID"],
                                 dfnew["SourceDataType"],
                                 dfnew["SourcePrecision"],
                                 dfnew["SourceScale"],
                                 dfnew["TargetId"],
                                 dfnew["TargetLayerName"],
                                 dfnew["TargetSchema"],
                                 dfnew["TargetDB"],
                                 dfnew["TargetServer"],
                                 dfnew["TargetProvider"],
                                 dfnew["TargetTable"],
                                 dfnew["TargetObjectType"],
                                 dfnew["TargetSql"],
                                 dfnew["TargetConnectionKey"],
                                 dfnew["TargetColumnName"],
                                 dfnew["TargetColumnID"],
                                 dfnew["TargetDataType"],
                                 dfnew["TargetPrecision"],
                                 dfnew["TargetScale"])


        return dfnew


    def getDfConnections(self,df):
        dfNew= df.select(df["mapName"],
                         df["id"],
                         df["path"],
                         df["connections_federatedId"],
                         df["connections_name"],
                         df["connections_host"],
                         df["connections_database"],
                         df["connections_schema"],
                         df["connections_type"]
                         ).distinct()
        dfNew=dfNew.withColumnRenamed("path","mapPath") \
                   .withColumnRenamed("id", "mapId") \
                   .withColumnRenamed("connections_federatedId", "connectionID")\
                   .withColumnRenamed("connections_host", "serverName") \
                    .withColumnRenamed("connections_database", "databaseName") \
                    .withColumnRenamed("connections_schema", "schemaName") \
                   .withColumnRenamed("connections_type", "connectionType") \
                   .withColumnRenamed("path", "mapPath")
        return dfNew

    def getTransformations(self,df):
        dfNew = df.select(df["mapName"],
                          df["id"],
                          df["path"],
                          df["map_content_transformations_ID"],
                          df["map_content_transformations_class"],
                          df["map_content_transformations_name"]
                          ).distinct()
        dfNew = dfNew.withColumnRenamed("path", "mapPath") \
            .withColumnRenamed("id", "mapId") \
            .withColumnRenamed("path", "mapPath")\
            .withColumnRenamed("map_content_transformations_ID", "transformations_ID") \
            .withColumnRenamed("map_content_transformations_class", "transformations_class") \
            .withColumnRenamed("map_content_transformations_name", "transformations_name") \


        return dfNew

    def getTransformationsDataObject(self,df):

        if ("map_content_transformations_manualMappings_ID" not in df.columns):
            df = df.withColumn("map_content_transformations_manualMappings_ID", lit(""))
        if ("map_content_transformations_manualMappings_class" not in df.columns):
            df = df.withColumn("map_content_transformations_manualMappings_class", lit(""))
        if ("map_content_transformations_manualMappings_mappingList_ID" not in df.columns):
            df = df.withColumn("map_content_transformations_manualMappings_mappingList_ID", lit(""))

        if ("map_content_transformations_manualMappings_mappingList_class" not in df.columns):
            df = df.withColumn("map_content_transformations_manualMappings_mappingList_class", lit(""))
        if ("map_content_transformations_manualMappings_mappingList_fromFieldName" not in df.columns):
            df = df.withColumn("map_content_transformations_manualMappings_mappingList_fromFieldName", lit(""))

        if ("map_content_transformations_manualMappings_mappingList_toField_ID" not in df.columns):
            df = df.withColumn("map_content_transformations_manualMappings_mappingList_toField_ID", lit(""))

        if ("map_content_transformations_manualMappings_mappingList_toField_class" not in df.columns):
            df = df.withColumn("map_content_transformations_manualMappings_mappingList_toField_class", lit(""))






        dfNew = df.select(df["mapName"],
                          df["id"],
                          df["path"],
                          df["map_content_transformations_ID"],
                          df["map_content_transformations_class"],
                          df["map_content_transformations_name"],
                          df["map_content_transformations_dataAdapter_object_ID"],
                          df["map_content_transformations_dataAdapter_object_class"],
                          df["map_content_transformations_dataAdapter_typeSystem"],
                          df["map_content_transformations_dataAdapter_object_label"],
                          df["map_content_transformations_dataAdapter_object_name"],
                          df["map_content_transformations_dataAdapter_object_path"],
                          df["map_content_transformations_dataAdapter_object_objectType"],
                          df["map_content_transformations_dataAdapter_connectionId"],
                          df["map_content_transformations_dataAdapter_object_customQuery"],
                          df["map_content_transformations_manualMappings_ID"],
                          df["map_content_transformations_manualMappings_class"],
                          df["map_content_transformations_manualMappings_mappingList_ID"],
                          df["map_content_transformations_manualMappings_mappingList_class"],
                          df["map_content_transformations_manualMappings_mappingList_fromFieldName"],
                          df["map_content_transformations_manualMappings_mappingList_toField_ID"],
                          df["map_content_transformations_manualMappings_mappingList_toField_class"]


                          ).distinct()

        dfNew = dfNew.withColumnRenamed("path", "mapPath") \
            .withColumnRenamed("id", "mapId") \
            .withColumnRenamed("path", "mapPath")\
            .withColumnRenamed("map_content_transformations_ID", "transformations_ID") \
            .withColumnRenamed("map_content_transformations_class", "transformations_class") \
            .withColumnRenamed("map_content_transformations_name", "transformations_name") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_ID", "tableID")\
            .withColumnRenamed("map_content_transformations_dataAdapter_object_class", "tableClass") \
            .withColumnRenamed("map_content_transformations_dataAdapter_typeSystem", "typeSystem") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_label", "object_label") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_name", "object_name") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_path", "object_path") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_objectType", "object_objectType") \
            .withColumnRenamed("map_content_transformations_dataAdapter_connectionId", "connectionId") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_customQuery", "manualMappingID") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_customQuery", "manualMappingClass")\
            .withColumnRenamed("map_content_transformations_dataAdapter_object_customQuery", "fromFieldName") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_customQuery", "toFieldID") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_customQuery", "toFieldClass")

        return dfNew




    def getTansfomrationColumnsData(self,df):
        dfNew = df.select(df["mapName"],
                          df["id"],
                          df["path"],
                          df["map_content_transformations_ID"],
                          df["map_content_transformations_class"],
                          df["map_content_transformations_name"],
                          df["map_content_transformations_dataAdapter_object_fields_ID"],
                          df["map_content_transformations_dataAdapter_object_fields_class"],
                          df["map_content_transformations_dataAdapter_object_fields_precision"],
                          df["map_content_transformations_dataAdapter_object_fields_scale"],
                          df["map_content_transformations_dataAdapter_object_fields_nativeName"],
                          df["map_content_transformations_dataAdapter_object_fields_nativeType"]
                          ).distinct()
        dfNew = dfNew.withColumnRenamed("path", "mapPath") \
            .withColumnRenamed("id", "mapId") \
            .withColumnRenamed("path", "mapPath") \
            .withColumnRenamed("map_content_transformations_ID", "transformations_ID") \
            .withColumnRenamed("map_content_transformations_class", "transformations_class") \
            .withColumnRenamed("map_content_transformations_name", "transformations_name") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_fields_ID", "field_ID_Adapter")\
            .withColumnRenamed("map_content_transformations_dataAdapter_object_fields_class", "field_class_Adapter")\
            .withColumnRenamed("map_content_transformations_dataAdapter_object_fields_precision", "DBPrecision") \
            .withColumnRenamed("map_content_transformations_dataAdapter_object_fields_scale", "DBScale")\
            .withColumnRenamed("map_content_transformations_dataAdapter_object_fields_nativeName", "nativeName")\
            .withColumnRenamed("map_content_transformations_dataAdapter_object_fields_nativeType", "nativeType")

        return dfNew

    def getTansfomrationColumns(self, df):
        if ("map_content_transformations_fields_expression" not in df.columns):
            df = df.withColumn("map_content_transformations_fields_expression", lit(""))

        dfNew = df.select(df["mapName"],
                          df["id"],
                          df["path"],
                          df["ProjectName"],
                          df["map_content_transformations_ID"],
                          df["map_content_transformations_class"],
                          df["map_content_transformations_name"],
                          df["map_content_transformations_fields_ID"],
                          df["map_content_transformations_fields_class"],
                          df["map_content_transformations_fields_name"],
                          df["map_content_transformations_fields_adapterField_ID"],
                          df["map_content_transformations_fields_adapterField_class"],
                          df["map_content_transformations_fields_platformType_SID"],
                          df["map_content_transformations_fields_expression"],
                          df["map_content_transformations_fields_precision"],
                          df["map_content_transformations_fields_scale"]

                          ).distinct()
        dfNew = dfNew.withColumnRenamed("path", "mapPath") \
            .withColumnRenamed("id", "mapId") \
            .withColumnRenamed("path", "mapPath") \
            .withColumnRenamed("map_content_transformations_ID", "transformations_ID") \
            .withColumnRenamed("map_content_transformations_class", "transformations_class") \
            .withColumnRenamed("map_content_transformations_name", "transformations_name") \
            .withColumnRenamed("map_content_transformations_fields_ID", "field_ID") \
            .withColumnRenamed("map_content_transformations_fields_class", "field_class") \
            .withColumnRenamed("map_content_transformations_fields_name", "column_Name") \
            .withColumnRenamed("map_content_transformations_fields_adapterField_ID", "field_ID_Data") \
            .withColumnRenamed("map_content_transformations_fields_adapterField_class", "field_ID_class")\
            .withColumnRenamed("map_content_transformations_fields_platformType_SID", "field_DataType") \
            .withColumnRenamed("map_content_transformations_fields_expression", "expression") \
            .withColumnRenamed("map_content_transformations_fields_precision", "precision") \
            .withColumnRenamed("map_content_transformations_fields_scale", "scale")

        return dfNew



    def getlinks(self,df):
        dfNew = df.select(df["mapName"],
                          df["id"],
                          df["ProjectName"],
                          df["path"],
                          df["map_content_links_ID"],
                          df["map_content_links_class"],
                          df["map_content_links_name"],
                          df["map_content_links_fromGroup_ID"],
                          df["map_content_links_fromGroup_class"],
                          df["map_content_links_fromTransformation_ID"],
                          df["map_content_links_fromTransformation_class"],
                          df["map_content_links_toGroup_ID"],
                          df["map_content_links_toGroup_class"],
                          df["map_content_links_toTransformation_ID"],
                          df["map_content_links_toTransformation_class"]
                          ).distinct()
        dfNew = dfNew.withColumnRenamed("path", "mapPath") \
            .withColumnRenamed("id", "mapId") \
            .withColumnRenamed("path", "mapPath") \
            .withColumnRenamed("id", "mapId") \
            .withColumnRenamed("path", "mapPath")\
            .withColumnRenamed("map_content_links_ID", "links_ID") \
            .withColumnRenamed("map_content_links_class", "link_class") \
            .withColumnRenamed("map_content_links_name", "link_name")\
            .withColumnRenamed("map_content_links_fromGroup_class", "fromGroup_class")\
            .withColumnRenamed("map_content_links_fromGroup_ID", "fromGroup_ID")\
            .withColumnRenamed("map_content_links_fromTransformation_ID", "fromTransformation_ID")\
            .withColumnRenamed("map_content_links_fromTransformation_class", "fromTransformation_class")\
            .withColumnRenamed("map_content_links_toGroup_ID", "toGroup_ID")\
            .withColumnRenamed("map_content_links_toGroup_class", "toGroup_class")\
            .withColumnRenamed("map_content_links_toTransformation_ID", "toTransformation_ID") \
            .withColumnRenamed("map_content_links_toTransformation_class", "toTransformation_class")


        return dfNew

    def createSourceToTarget(self,links,
                             connectionsDF,
                             transformationdf
                             ,columntransData
                             ,columntrans,
                              dataobject,
                              dtexpinner,
                              manualMappingDF,
                              colsDF
                             ):
        cnSource, cnTarget, dataObjectSource, dataObjectTarget, links, transSource, transTarget = self.createDFBasics(
            columntrans, columntransData, connectionsDF, dataobject, links, transformationdf)

        #joins link with source transformations
        dfJoin = self.createDFJoin(links, transSource)

        # joins link with target transformations
        dfJoin2 = self.createDFJoin2(dfJoin, transTarget)
        # #joins links with source dataobject
        dfJoin3 = self.createDFJoin3(dataObjectSource, dfJoin2)
        dfJoin4 = self.createDFJoin4(dataObjectTarget, dfJoin3)

        cnSource = cnSource.drop("mapName").drop("mapId").drop("mapPath")
        dfJoin5 = self.createDFJoin5(cnSource, dfJoin4)

        #print("cnTarget")
        #cnTarget.show(1)
        cnTarget=cnTarget.drop("mapName").drop("mapId").drop("mapPath")
        dfJoin6 = self.createDFJoin6(cnSource, cnTarget, dfJoin5)

        dfSourceToTarget = self.createDFSTT(dfJoin6)

        colTransData = columntransData.alias('colTransData')
        columnTrans = columntrans.alias('columnTrans')

        print("colTransData:")
        #colTransData.show(1)

        print("columnTrans")
        #columnTrans.show(1)

        dfSourceToTargetColsSource = self.createDFSTTColsSource(colTransData, dfSourceToTarget)

        dfExpSource=self.createDFSTTColsEx(dfSourceToTarget, dfSourceToTargetColsSource)


        dfSourceToTargetColsTarget = self.createDFSTTColsTarget(colTransData, dfSourceToTarget)

        dfExpTarget = self.createDFSTTColsExT(dfSourceToTarget, dfSourceToTargetColsTarget)

        #join source and targets

        dfSourceToTargetColFinal = self.createDFSTTFinal\
            (dfSourceToTargetColsSource, dfSourceToTargetColsTarget,dfExpSource,dfExpTarget,dtexpinner,
             manualMappingDF,
             colsDF
             )

        dfSourceToTargetColFinal=dfSourceToTargetColFinal.\
        filter(dfSourceToTargetColFinal.TargetColumnName.isNotNull())

        return dfSourceToTargetColFinal

    def createDFJoin6(self, cnSource, cnTarget, dfJoin5):
        dfJoin6 = dfJoin5.join(cnSource, dfJoin5['TargetConnID'] == cnTarget['connectionID'], "left") \
            .withColumnRenamed("connections_name", "TargetConnectionKey") \
            .withColumnRenamed("databaseName", "TargetDb") \
            .withColumnRenamed("schemaName", "TargetSchema") \
            .withColumnRenamed("serverName", "TargetServer")
        print("dfJoin6")
        # dfJoin6.show(1)
        return dfJoin6

    def createDFSTTFinal(self,
                         dfSourceToTargetColsSource,
                         dfSourceToTargetColsTarget,
                         dfExpSource,
                         dfExpTarget,
                         dtexpinner,
                         manualMappingDF,
                         colsDF
                         ):

        dfSourceToTargetColsTarget=dfSourceToTargetColsTarget.drop("ContainerObjectName")\
        .drop("ContainerObjectPath").drop("ControlflowName").drop("ControlflowPath")

        dfSourceToTargetColFinal = dfSourceToTargetColsSource.join(dfSourceToTargetColsTarget, \
                                                                   (dfSourceToTargetColsSource['TargetIdC'] ==
                                                                    dfSourceToTargetColsTarget[
                                                                        'TargetId']) & \
                                                                   (dfSourceToTargetColsSource['mapId'] ==
                                                                    dfSourceToTargetColsTarget[
                                                                        "mapIdC"]) & \
                                                                   (dfSourceToTargetColsSource['SourceColumnName'] ==
                                                                    dfSourceToTargetColsTarget["TargetColumnName"])
                                                                   , "left")\
            .drop("mapIdC").drop("TargetIdC").drop("TargetLayerNameC") \
            .drop("TargetSchemaC").drop("TargetDBC").drop("TargetServerC").drop("TargetProviderC").drop("TargetTableC") \
            .drop("TargetSqlC").drop("TargetConnectionKeyC").drop("dfSourceToTargetColsTarget.ContainerObjectName")\

        #print("DF1:")
        #dfSourceToTargetColFinal.show(1)
        # print("Df2:")
        # dfExpSource.show(1)


        #print("Df3:")
        #dfExpTarget.show(1)
        dfManual= self.createManualMap(  manualMappingDF,
                              colsDF,dfExpTarget)

        # print("Df4:")
        # dfManual.show(1)

        dfSourceToTargetColFinal = dfSourceToTargetColFinal.unionAll(dfExpSource)
        dfSourceToTargetColFinal=dfSourceToTargetColFinal.unionAll(dfExpTarget)
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.unionAll(dtexpinner)
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.unionAll(dfManual)

        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("SourceIsObjectData", \
                                                                       expr(
                                                                           "case when SourceObjectType in ('Source','Target') then '1' " \
                                                                           "else '0' end"))
        # set objectType
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("SourceObjectType", \
                                                                       when(
                                                                           dfSourceToTargetColFinal.SourceLayerName == "Expression",
                                                                           "Expression")\
                                                                       .otherwise(
                                                                           "Source"))
        #
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("TargetObjectType", \
                                                                       when(
                                                                           dfSourceToTargetColFinal.TargetLayerName == "Expression",
                                                                           "Expression") \
                                                                         .otherwise(
                                                                           "Target"))
        # set isObjectData
        dfSourceToTargetColFinal = dfSourceToTargetColFinal. \
            filter(dfSourceToTargetColFinal.SourceColumnName.isNotNull())
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("SourceIsObjectData", \
                                                                       expr(
                                                                           "case when SourceObjectType in ('Source','Target') then '1' " \
                                                                           "else '0' end"))
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("TargetIsObjectData", \
                                                                       expr(
                                                                           "case when TargetObjectType in ('Source','Target') then '1' " \
                                                                           "else '0' end"))
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("SourceColumnDataId", \
                                                                       when(
                                                                           dfSourceToTargetColFinal.SourceIsObjectData == "1",
                                                                           concat( \
                                                                               dfSourceToTargetColFinal["SourceServer"], \
                                                                               dfSourceToTargetColFinal["SourceDB"], \
                                                                               dfSourceToTargetColFinal["SourceSchema"], \
                                                                               dfSourceToTargetColFinal["SourceTable"], \
                                                                               dfSourceToTargetColFinal[
                                                                                   "SourceColumnName"]) \
                                                                           ).otherwise("-1"))
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("TargetColumnDataId", \
                                                                       when(
                                                                           dfSourceToTargetColFinal.TargetIsObjectData == "1",
                                                                           concat( \
                                                                               dfSourceToTargetColFinal["TargetServer"], \
                                                                               dfSourceToTargetColFinal["TargetDB"], \
                                                                               dfSourceToTargetColFinal["TargetSchema"], \
                                                                               dfSourceToTargetColFinal["TargetTable"], \
                                                                               dfSourceToTargetColFinal[
                                                                                   "TargetColumnName"])
                                                                           ).otherwise("-1"))
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("SourceColumnMapId", \
                                                                       concat(
                                                                           dfSourceToTargetColFinal[
                                                                               "ContainerObjectName"], \
                                                                           dfSourceToTargetColFinal[
                                                                               "ContainerObjectPath"], \
                                                                           dfSourceToTargetColFinal["ControlflowName"], \
                                                                           dfSourceToTargetColFinal["ControlflowPath"], \
                                                                           dfSourceToTargetColFinal["SourceLayerName"], \
                                                                           dfSourceToTargetColFinal["SourceObjectType"], \
                                                                           dfSourceToTargetColFinal["SourceColumnName"]) \
                                                                       )
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("TargetColumnMapId", \
                                                                       concat(dfSourceToTargetColFinal[
                                                                                  "ContainerObjectName"], \
                                                                              dfSourceToTargetColFinal[
                                                                                  "ContainerObjectPath"], \
                                                                              dfSourceToTargetColFinal[
                                                                                  "ControlflowName"], \
                                                                              dfSourceToTargetColFinal[
                                                                                  "ControlflowPath"], \
                                                                              dfSourceToTargetColFinal[
                                                                                  "TargetLayerName"], \
                                                                              dfSourceToTargetColFinal[
                                                                                  "TargetObjectType"], \
                                                                              dfSourceToTargetColFinal[
                                                                                  "TargetColumnName"]) \
                                                                       )
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("SourceColumnMapGuid", \
                                                                       md5(dfSourceToTargetColFinal[
                                                                               "SourceColumnMapId"]))
        #
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("SourceColumnDataGuid", \
                                                                       md5(dfSourceToTargetColFinal[
                                                                               "SourceColumnDataId"]))
        #
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("TargetColumnMapGuid", \
                                                                       md5(dfSourceToTargetColFinal[
                                                                               "TargetColumnMapId"]))
        #
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("TargetColumnDataGuid", \
                                                                       md5(dfSourceToTargetColFinal[
                                                                               "TargetColumnDataId"]))
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("ContainerObjectType", \
                                                                       lit("Map"))
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("ContainerToolType", \
                                                                       lit("Informatica Cloud"))
        dfSourceToTargetColFinal = dfSourceToTargetColFinal.withColumn("ToolType", \
                                                                       lit("ETL"))
        return dfSourceToTargetColFinal

    def createDFSTTColsTarget(self, colTransData, dfSourceToTarget):
        dfSourceToTargetColsTarget = dfSourceToTarget.join(colTransData, \
                                                           (dfSourceToTarget['TargetId'] == colTransData[
                                                               'transformations_ID']) & \
                                                           (dfSourceToTarget['TargetObjectType'] == colTransData[
                                                               "transformations_class"]) & \
                                                           (dfSourceToTarget['mapId'] == colTransData["mapId"])
                                                           , "left") \
            .withColumnRenamed("nativeName", "TargetColumnName") \
            .withColumnRenamed("field_ID_Adapter", "TargetColumnID") \
            .withColumnRenamed("nativeType", "TargetDataType") \
            .withColumnRenamed("DBPrecision", "TargetPrecision") \
            .withColumnRenamed("DBScale", "TargetScale")
        dfSourceToTargetColsTarget = dfSourceToTargetColsTarget. \
            select(

            dfSourceToTargetColsTarget["ContainerObjectName"],
            dfSourceToTargetColsTarget["ContainerObjectPath"],
            dfSourceToTargetColsTarget["ControlflowName"],
            dfSourceToTargetColsTarget["links.mapId"],
            dfSourceToTargetColsTarget["ControlflowPath"],
            dfSourceToTargetColsTarget["TargetId"],
            dfSourceToTargetColsTarget["TargetLayerName"],
            dfSourceToTargetColsTarget["TargetSchema"],
            dfSourceToTargetColsTarget["TargetDB"],
            dfSourceToTargetColsTarget["TargetServer"],
            dfSourceToTargetColsTarget["TargetProvider"],
            dfSourceToTargetColsTarget["TargetTable"],
            dfSourceToTargetColsTarget["TargetObjectType"],
            dfSourceToTargetColsTarget["TargetSql"],
            dfSourceToTargetColsTarget["TargetConnectionKey"],
            dfSourceToTargetColsTarget["TargetColumnName"],
            dfSourceToTargetColsTarget["TargetColumnID"],
            dfSourceToTargetColsTarget["TargetDataType"],
            dfSourceToTargetColsTarget["TargetPrecision"],
            dfSourceToTargetColsTarget["TargetScale"]

        ).distinct()
        print("dfSourceToTargetColsTarget")
        #dfSourceToTargetColsTarget.show(1)
        dfSourceToTargetColsTarget = dfSourceToTargetColsTarget.withColumnRenamed("mapId", "mapIdC")
        return dfSourceToTargetColsTarget

    def createDFSTTColsEx(self, dfSourceToTarget, dfSourceToTargetColsSource):
        dfSourceToTargetEx = dfSourceToTarget.filter(dfSourceToTarget.TargetLayerName == "Expression")
        dfSourceToTargetEx = dfSourceToTargetEx.drop("ContainerObjectName").drop("ContainerObjectPath") \
            .drop("ControlflowName").drop("ControlflowPath").withColumnRenamed("mapId", "mapIdT")
        print("dfSourceToTargetEx:")
        dfSourceToTargetEx=dfSourceToTargetEx.withColumnRenamed("SourceId","SourceIdC")\
        .withColumnRenamed("SourceObjectType","SourceObjectTypeC")\
        .withColumnRenamed("SourceDB","SourceDBC") \
        .withColumnRenamed("SourceServer", "SourceServerC")\
        .withColumnRenamed("SourceProvider", "SourceProviderC") \
        .withColumnRenamed("SourceTable", "SourceTableC") \
        .withColumnRenamed("SourceSql", "SourceSqlC") \
        .withColumnRenamed("SourceConnectionKey", "SourceConnectionKeyC") \
        .withColumnRenamed("SourceLayerName", "SourceLayerNameC") \
        .withColumnRenamed("SourceSchema", "SourceSchemaC")




        #dfSourceToTargetEx.show(1)
        dfSToTColsSourceEx = dfSourceToTargetColsSource.join(dfSourceToTargetEx, \
                                                             (dfSourceToTargetEx["SourceIdC"] ==
                                                              dfSourceToTargetColsSource[
                                                                  "SourceId"]) & \
                                                             (dfSourceToTargetEx["SourceObjectTypeC"] ==
                                                              dfSourceToTargetColsSource[
                                                                  "SourceObjectType"]) & \
                                                             (dfSourceToTargetEx["mapIdT"] ==
                                                              dfSourceToTargetColsSource[
                                                                  "mapId"]), "inner")
        print("dfSToTColsSourceEx before:")
        #dfSToTColsSourceEx.show(1)
        # create dfSToTColsSourceEx same as dfSourceToTargetColsSource but the source is expression
        # inherits the columns from the source component
        dfSToTColsSourceEx = dfSToTColsSourceEx.select(
        dfSToTColsSourceEx["ContainerObjectName"],
        dfSToTColsSourceEx["ContainerObjectPath"],
        dfSToTColsSourceEx["ControlflowName"],
        dfSToTColsSourceEx["mapId"],
        dfSToTColsSourceEx["ControlflowPath"],
        dfSToTColsSourceEx["SourceId"],  # as
        dfSToTColsSourceEx["SourceLayerName"],  # as
        dfSToTColsSourceEx["SourceSchema"],  # as
        dfSToTColsSourceEx["SourceDB"],  # as
        dfSToTColsSourceEx["SourceServer"],  # as
        dfSToTColsSourceEx["SourceProvider"],  # as
        dfSToTColsSourceEx["SourceTable"],  # as
        dfSToTColsSourceEx["SourceObjectType"],  # as
        dfSToTColsSourceEx["SourceSql"],  # as
        dfSToTColsSourceEx["SourceConnectionKey"],  # as
        dfSToTColsSourceEx["SourceColumnName"],
        dfSToTColsSourceEx["SourceColumnID"],
        dfSToTColsSourceEx["SourceDataType"],
        dfSToTColsSourceEx["SourcePrecision"],
        dfSToTColsSourceEx["SourceScale"],  # as
        dfSToTColsSourceEx["TargetId"],
        dfSToTColsSourceEx["TargetLayerName"],
        dfSToTColsSourceEx["TargetSchema"],
        dfSToTColsSourceEx["TargetDB"],
        dfSToTColsSourceEx["TargetServer"],
        dfSToTColsSourceEx["TargetProvider"],
        dfSToTColsSourceEx["TargetTable"],
        dfSToTColsSourceEx["TargetObjectType"],  # as
        dfSToTColsSourceEx["TargetSql"],
        dfSToTColsSourceEx["TargetConnectionKey"]).distinct() \
        .withColumn("TargetColumnName",dfSToTColsSourceEx["SourceColumnName"]) \
        .withColumn("TargetColumnID", dfSToTColsSourceEx["SourceColumnID"]) \
        .withColumn("TargetDataType", dfSToTColsSourceEx["SourceDataType"]) \
        .withColumn("TargetPrecision", dfSToTColsSourceEx["SourcePrecision"]) \
        .withColumn("TargetScale", dfSToTColsSourceEx["SourceScale"]) \



        print("dfSToTColsSourceEx")
        #dfSToTColsSourceEx.show(1)
        return dfSToTColsSourceEx

    def createDFSTTColsExT(self, dfSourceToTarget, dfSourceToTargetColsTarget):
        dfSourceToTargetEx = dfSourceToTarget.filter(dfSourceToTarget.SourceLayerName == "Expression")
        dfSourceToTargetEx = dfSourceToTargetEx.drop("ContainerObjectName").drop("ContainerObjectPath") \
            .drop("ControlflowName").drop("ControlflowPath").withColumnRenamed("mapId", "mapIdT")

        dfSourceToTargetEx = dfSourceToTargetEx.withColumnRenamed("TargetId", "TargetIdC") \
            .withColumnRenamed("TargetObjectType", "TargetObjectTypeC") \
            .withColumnRenamed("TargetLayerName", "TargetLayerNameC")\
            .withColumnRenamed("TargetDB", "TargetDBC") \
            .withColumnRenamed("TargetServer", "SourceServerC") \
            .withColumnRenamed("TargetLayerName", "TargetLayerNameC") \
            .withColumnRenamed("TargetSchema", "TargetSchemaC") \
            .withColumnRenamed("TargetId", "TargetIdC") \
            .withColumnRenamed("TargetProvider", "TargetProviderC") \
            .withColumnRenamed("TargetTable", "TargetTableC") \
            .withColumnRenamed("TargetObjectType", "TargetObjectTypeC") \
            .withColumnRenamed("TargetSql", "TargetSqlC") \
            .withColumnRenamed("TargetConnectionKey", "TargetConnectionKeyC")
            # .withColumnRenamed("SourceProvider", "SourceProviderC") \
            # .withColumnRenamed("SourceTable", "SourceTableC") \
            # .withColumnRenamed("SourceServer", "SourceServerC") \
            # .withColumnRenamed("SourceSql", "SourceSqlC") \
            # .withColumnRenamed("SourceConnectionKey", "SourceConnectionKeyC") \
            # .withColumnRenamed("SourceLayerName", "SourceLayerNameC") \
            # .withColumnRenamed("SourceSchema", "SourceSchemaC")

        # print("dfSourceToTargetEx:")
        # dfSourceToTargetEx.show(1)
        # print ("dfSourceToTargetColsTarget")
        # dfSourceToTargetColsTarget.show(1)
        dfSToTColsTargetEx = dfSourceToTargetColsTarget.join(dfSourceToTargetEx, \
                                                             (dfSourceToTargetEx["TargetIdC"] ==
                                                              dfSourceToTargetColsTarget[
                                                                  "TargetId"]) & \
                                                             (dfSourceToTargetEx["TargetObjectTypeC"] ==
                                                              dfSourceToTargetColsTarget[
                                                                  "TargetObjectType"]) & \
                                                             (dfSourceToTargetEx["mapIdT"] ==
                                                              dfSourceToTargetColsTarget[
                                                                  "mapIdC"]), "inner")
        # print("dfSToTColsTargetEx before:")
        # dfSToTColsTargetEx.show(1)
        # create dfSToTColsSourceEx same as dfSourceToTargetColsSource but the source is expression
        # inherits the columns from the source component
        dfSToTColsTargetEx=dfSToTColsTargetEx.\
             withColumn("SourceColumnName", dfSToTColsTargetEx["TargetColumnName"]) \
            .withColumn("SourceColumnID", dfSToTColsTargetEx["TargetColumnID"]) \
            .withColumn("SourceDataType", dfSToTColsTargetEx["TargetDataType"]) \
            .withColumn("SourcePrecision", dfSToTColsTargetEx["TargetPrecision"]) \
            .withColumn("SourceScale", dfSToTColsTargetEx["TargetScale"]) \
            .withColumnRenamed("mapIdC", "mapId")

        dfSToTColsTargetEx = dfSToTColsTargetEx.select(
            dfSToTColsTargetEx["ContainerObjectName"],
            dfSToTColsTargetEx["ContainerObjectPath"],
            dfSToTColsTargetEx["ControlflowName"],
            dfSToTColsTargetEx["mapId"],
            dfSToTColsTargetEx["ControlflowPath"],
            dfSToTColsTargetEx["SourceId"],  # as
            dfSToTColsTargetEx["SourceLayerName"],  # as
            dfSToTColsTargetEx["SourceSchema"],  # as
            dfSToTColsTargetEx["SourceDB"],  # as
            dfSToTColsTargetEx["SourceServer"],  # as
            dfSToTColsTargetEx["SourceProvider"],  # as
            dfSToTColsTargetEx["SourceTable"],  # as
            dfSToTColsTargetEx["SourceObjectType"],  # as
            dfSToTColsTargetEx["SourceSql"],  # as
            dfSToTColsTargetEx["SourceConnectionKey"],  # as
            dfSToTColsTargetEx["SourceColumnName"],
            dfSToTColsTargetEx["SourceColumnID"],
            dfSToTColsTargetEx["SourceDataType"],
            dfSToTColsTargetEx["SourcePrecision"],
            dfSToTColsTargetEx["SourceScale"],
            dfSToTColsTargetEx["TargetId"],
            dfSToTColsTargetEx["TargetLayerName"],
            dfSToTColsTargetEx["TargetSchema"],
            dfSToTColsTargetEx["TargetDB"],
            dfSToTColsTargetEx["TargetServer"],
            dfSToTColsTargetEx["TargetProvider"],
            dfSToTColsTargetEx["TargetTable"],
            dfSToTColsTargetEx["TargetObjectType"],  # as
            dfSToTColsTargetEx["TargetSql"],
            dfSToTColsTargetEx["TargetConnectionKey"],
            dfSToTColsTargetEx["TargetColumnName"],
            dfSToTColsTargetEx["TargetColumnID"],
            dfSToTColsTargetEx["TargetDataType"],
            dfSToTColsTargetEx["TargetPrecision"],
            dfSToTColsTargetEx["TargetScale"]).distinct() # as






        print("dfSToTColsTargetEx")
        #dfSToTColsTargetEx.show(1)
        return dfSToTColsTargetEx


    def createDFSTTColsSource(self, colTransData, dfSourceToTarget):
        dfSourceToTargetColsSource = dfSourceToTarget.join(colTransData, \
                                                           (dfSourceToTarget['SourceId'] == colTransData[
                                                               'transformations_ID']) & \
                                                           (dfSourceToTarget['SourceObjectType'] == colTransData[
                                                               "transformations_class"]) & \
                                                           (dfSourceToTarget['mapId'] == colTransData["mapId"])
                                                           , "inner") \
            .withColumnRenamed("nativeName", "SourceColumnName") \
            .withColumnRenamed("field_ID_Adapter", "SourceColumnID") \
            .withColumnRenamed("nativeType", "SourceDataType") \
            .withColumnRenamed("DBPrecision", "SourcePrecision") \
            .withColumnRenamed("DBScale", "SourceScale")
        dfSourceToTargetColsSource = dfSourceToTargetColsSource. \
            select(
                   dfSourceToTargetColsSource["ContainerObjectName"],
                   dfSourceToTargetColsSource["ContainerObjectPath"],
                   dfSourceToTargetColsSource["ControlflowName"],
                   dfSourceToTargetColsSource["links.mapId"],
                   dfSourceToTargetColsSource["ControlflowPath"],
                   dfSourceToTargetColsSource["SourceId"],
                   dfSourceToTargetColsSource["SourceLayerName"],
                   dfSourceToTargetColsSource["SourceSchema"],
                   dfSourceToTargetColsSource["SourceDB"],
                   dfSourceToTargetColsSource["SourceServer"],
                   dfSourceToTargetColsSource["SourceProvider"],
                   dfSourceToTargetColsSource["SourceTable"],
                   dfSourceToTargetColsSource["SourceObjectType"],
                   dfSourceToTargetColsSource["SourceSql"],
                   dfSourceToTargetColsSource["SourceConnectionKey"],
                   dfSourceToTargetColsSource["SourceColumnName"],
                   dfSourceToTargetColsSource["SourceColumnID"],
                   dfSourceToTargetColsSource["SourceDataType"],
                   dfSourceToTargetColsSource["SourcePrecision"],
                   dfSourceToTargetColsSource["SourceScale"],
                   dfSourceToTargetColsSource["TargetId"],
                   dfSourceToTargetColsSource["TargetLayerName"],
                   dfSourceToTargetColsSource["TargetSchema"],
                   dfSourceToTargetColsSource["TargetDB"],
                   dfSourceToTargetColsSource["TargetServer"],
                   dfSourceToTargetColsSource["TargetProvider"],
                   dfSourceToTargetColsSource["TargetTable"],
                   dfSourceToTargetColsSource["TargetSql"],
                   dfSourceToTargetColsSource["TargetConnectionKey"]
                   ).distinct() \
            .withColumnRenamed("TargetId", "TargetIdC") \
            .withColumnRenamed("TargetLayerName", "TargetLayerNameC") \
            .withColumnRenamed("TargetSchema", "TargetSchemaC") \
            .withColumnRenamed("TargetDB", "TargetDBC") \
            .withColumnRenamed("TargetServer", "TargetServerC") \
            .withColumnRenamed("TargetProvider", "TargetProviderC") \
            .withColumnRenamed("TargetTable", "TargetTableC") \
            .withColumnRenamed("TargetObjectType", "TargetObjectTypeC") \
            .withColumnRenamed("TargetSql", "TargetSqlC") \
            .withColumnRenamed("TargetConnectionKey", "TargetConnectionKeyC")
        dfSourceToTargetColsSource = dfSourceToTargetColsSource.filter(dfSourceToTargetColsSource.SourceColumnName.
                                                                       isNotNull())

        print("dfSourceToTargetColsSource")
        #dfSourceToTargetColsSource.show(1)
        return dfSourceToTargetColsSource

    def createDFSTT(self, dfJoin6):
        dfSourceToTarget = dfJoin6.select(dfJoin6["ProjectName"],
                                          dfJoin6["links.mapPath"],
                                          dfJoin6["links.mapId"],
                                          dfJoin6["links.mapName"],
                                          dfJoin6["From_trID"],
                                          dfJoin6["SourceLayerName"],
                                          dfJoin6["SourceSchema"],
                                          dfJoin6["SourceDB"],
                                          dfJoin6["SourceServer"],
                                          dfJoin6["SourceProvider"],
                                          dfJoin6["SourceTable"],
                                          dfJoin6["From_class"],
                                          dfJoin6["SourceSql"],
                                          dfJoin6["SourceConnectionKey"],
                                          dfJoin6["To_trID"],
                                          dfJoin6["TargetLayerName"],
                                          dfJoin6["TargetSchema"],
                                          dfJoin6["TargetDB"],
                                          dfJoin6["TargetServer"],
                                          dfJoin6["TargetProvider"],
                                          dfJoin6["TargetTable"],
                                          dfJoin6["To_class"],
                                          dfJoin6["TargetSql"],
                                          dfJoin6["TargetConnectionKey"]

                                          ) \
            .withColumn("ContainerObjectType", lit("Map")) \
            .withColumn("ContainerToolType", lit("InformaticaCloud")) \
            .withColumn("ControlflowPath", dfJoin6["links.mapName"]) \
            .withColumnRenamed("ProjectName", "ContainerObjectName") \
            .withColumnRenamed("mapName", "ControlflowName") \
            .withColumnRenamed("mapPath", "ContainerObjectPath") \
            .withColumnRenamed("From_trID", "SourceId") \
            .withColumnRenamed("From_class", "SourceObjectType") \
            .withColumnRenamed("To_trID", "TargetId") \
            .withColumnRenamed("To_class", "TargetObjectType")
        # Add columns to each component
        print("dfSourceToTarget")
        #dfSourceToTarget.show(1)
        return dfSourceToTarget

    def createDFJoin5(self, cnSource, dfJoin4):
        dfJoin5 = dfJoin4.join(cnSource, dfJoin4['SourceConnID'] == cnSource['connectionID'], "left") \
            .withColumnRenamed("connections_name", "SourceConnectionKey") \
            .withColumnRenamed("databaseName", "SourceDb") \
            .withColumnRenamed("schemaName", "SourceSchema") \
            .withColumnRenamed("serverName", "SourceServer")
        #
        if ("SourceSql" not in dfJoin5.columns):
            dfJoin5 = dfJoin5.withColumn("SourceSql", lit(""))
        if ("TargetSql" not in dfJoin5.columns):
            dfJoin5 = dfJoin5.withColumn("TargetSql", lit(""))
        dfJoin5 = dfJoin5.select(dfJoin5["links.mapName"],
                                 dfJoin5["ProjectName"],
                                 dfJoin5["links.mapId"],
                                 dfJoin5["links.mapPath"],
                                 dfJoin5["From_trID"],
                                 dfJoin5["From_class"],
                                 dfJoin5["SourceConnectionKey"],
                                 dfJoin5["SourceDB"],
                                 dfJoin5["SourceServer"],
                                 dfJoin5["SourceSchema"],
                                 dfJoin5["SourceLayerName"],
                                 dfJoin5["SourceConnID"],
                                 dfJoin5["SourceTable"],
                                 dfJoin5["SourceSql"],
                                 dfJoin5["SourceProvider"],
                                 dfJoin5["TargetConnID"],
                                 dfJoin5["link_name"],
                                 dfJoin5["To_trID"],
                                 dfJoin5["To_class"],
                                 dfJoin5["TargetTable"],
                                 dfJoin5["TargetSql"],
                                 dfJoin5["TargetProvider"],
                                 dfJoin5["TargetLayerName"])
        #
        print("dfJoin5")
        # dfJoin5.show(1)
        return dfJoin5

    def createDFJoin4(self, dataObjectTarget, dfJoin3):
        dfJoin4 = dfJoin3.join(dataObjectTarget, (dfJoin3['To_trID'] == dataObjectTarget['transformations_ID']) \
                               & (dfJoin3['To_class'] == dataObjectTarget['transformations_class']), "inner")
        if ("customQuery" not in dfJoin4.columns):
            dfJoin4 = dfJoin4.withColumn("customQuery", lit(""))
        dfJoin4 = dfJoin4.select(dfJoin4["links.mapName"],
                                 dfJoin4["ProjectName"],
                                 dfJoin4["links.mapId"],
                                 dfJoin4["links.mapPath"],
                                 dfJoin4["From_trID"],
                                 dfJoin4["From_class"],
                                 dfJoin4["SourceLayerName"],
                                 dfJoin4["SourceConnID"],
                                 dfJoin4["SourceTable"],
                                 dfJoin4["SourceSql"],
                                 dfJoin4["SourceProvider"],
                                 dfJoin4["connectionId"],
                                 dfJoin4["link_name"],
                                 dfJoin4["To_trID"],
                                 dfJoin4["To_class"],
                                 dfJoin4["object_name"],
                                 dfJoin4["customQuery"],
                                 dfJoin4["typeSystem"],
                                 dfJoin4["TargetLayerName"]
                                 ) \
            .withColumnRenamed("connectionId", "TargetConnID") \
            .withColumnRenamed("object_name", "TargetTable") \
            .withColumnRenamed("typeSystem", "TargetProvider") \
            .withColumnRenamed("customQuery", "TargetSql") \
            .withColumn('SourceConnID', regexp_replace('SourceConnID', 'saas:@', '')) \
            .withColumn('TargetConnID', regexp_replace('TargetConnID', 'saas:@', ''))
        # #join with sourceConnetion
        print("dfJoin4")
        # dfJoin4.show(1)
        return dfJoin4

    def createDFJoin3(self, dataObjectSource, dfJoin2):
        dfJoin3 = dfJoin2.join(dataObjectSource, (dfJoin2['From_trID'] == dataObjectSource['transformations_ID']) \
                               & (dfJoin2['From_class'] == dataObjectSource['transformations_class']), "inner")
        # #reanme
        #print(dfJoin3.columns)
        if ("customQuery" not in dfJoin3.columns):
            dfJoin3 = dfJoin3.withColumn("customQuery", lit(""))
        dfJoin3 = dfJoin3.select(dfJoin3["links.mapName"],
                                 dfJoin3["ProjectName"],
                                 dfJoin3["links.mapId"],
                                 dfJoin3["links.mapPath"],
                                 dfJoin3["From_trID"],
                                 dfJoin3["From_class"],
                                 dfJoin3["SourceLayerName"],
                                 dfJoin3["object_name"],
                                 dfJoin3["typeSystem"],
                                 dfJoin3["customQuery"],
                                 dfJoin3["connectionId"],
                                 dfJoin3["link_name"],
                                 dfJoin3["To_trID"],
                                 dfJoin3["To_class"],
                                 dfJoin3["TargetLayerName"]
                                 ) \
            .withColumnRenamed("connectionId", "SourceConnID") \
            .withColumnRenamed("object_name", "SourceTable") \
            .withColumnRenamed("typeSystem", "SourceProvider") \
            .withColumnRenamed("customQuery", "SourceSql")
        # # joins links with target dataobject
        print("dfJoin3")
        # dfJoin3.show(1)
        return dfJoin3

    def createDFJoin2(self, dfJoin, transTarget):
        dfJoin2 = dfJoin.join(transTarget, (dfJoin['To_trID'] == transTarget['transformations_ID']) \
                              & (dfJoin['To_class'] == transTarget['transformations_class']), "inner")
        # #rename columns
        dfJoin2 = dfJoin2.select(dfJoin2["links.mapName"],
                                 dfJoin2["ProjectName"],
                                 dfJoin2["links.mapId"],
                                 dfJoin2["links.mapPath"],
                                 dfJoin2["From_trID"],
                                 dfJoin2["From_class"],
                                 dfJoin2["SourceLayerName"],
                                 dfJoin2["link_name"],
                                 dfJoin2["To_trID"],
                                 dfJoin2["To_class"],
                                 dfJoin2["transformations_name"]
                                 ). \
            withColumnRenamed("transformations_name", "TargetLayerName")
        print("dfJoin2")
        # dfJoin2.show(1)
        return dfJoin2

    def createDFJoin(self, links, transSource):
        dfJoin = links.join(transSource, (links['fromTransformation_ID'] == transSource['transformations_ID']) \
                            & (links['fromTransformation_class'] == transSource['transformations_class']), "inner")
        # rename columns
        dfJoin = dfJoin.select(dfJoin["links.mapName"],
                               dfJoin["ProjectName"],
                               dfJoin["links.mapId"],
                               dfJoin["links.mapPath"],
                               dfJoin["transformations_name"],
                               dfJoin["fromTransformation_ID"],
                               dfJoin["fromTransformation_class"],
                               dfJoin["link_name"],
                               dfJoin["toTransformation_ID"],
                               dfJoin["toTransformation_class"],
                               ). \
            withColumnRenamed("transformations_name", "SourceLayerName") \
            .withColumnRenamed("toTransformation_ID", "To_trID") \
            .withColumnRenamed("toTransformation_class", "To_class") \
            .withColumnRenamed("fromTransformation_ID", "From_trID") \
            .withColumnRenamed("fromTransformation_class", "From_class")
        print("dfJoin")
        # dfJoin.show(1)
        return dfJoin

    def createDFBasics(self, columntrans, columntransData, connectionsDF, dataobject, links, transformationdf):
        cnSource = connectionsDF.alias('cnSource')
        cnTarget = connectionsDF.alias('cnTarget')
        transSource = transformationdf.alias('transSource')
        transTarget = transformationdf.alias('transTarget')
        colTransData = columntransData.alias('colTransData')
        columnTrans = columntrans.alias('columnTrans')
        dataObjectSource = dataobject.alias('dataObjectSource')
        dataObjectTarget = dataobject.alias('dataObjectTarget')
        links = links.alias("links")
        return cnSource, cnTarget, dataObjectSource, dataObjectTarget, links, transSource, transTarget

    def md5(str2hash):
        return hashlib.md5(str2hash.encode())

    md5UDF = udf(lambda z: md5(z), StringType())

    def createManualMap(self, manualMappingDF, colsDF, dfExpTarget):

        manualMappingDF=manualMappingDF.withColumnRenamed("mapId","mapIdC")

        dt=manualMappingDF.join(colsDF,
                                (manualMappingDF['mapIdC'] == colsDF['mapId'])\
                                 & (manualMappingDF['TargetId'] == colsDF['transformations_ID']) \
                                & (manualMappingDF['toField_ID'] == colsDF['field_ID'])
                                , "inner")

        dt=dt.withColumnRenamed("mapId","mapIdD")\
            .withColumnRenamed("TargetId", "TargetIdC") \
            .withColumnRenamed("TargetLayerName", "TargetLayerNameC") \

        dfNewRow=dt.join(dfExpTarget,
                         (dt['mapIdC'] == dfExpTarget['mapId']) \
                         & (dt['TargetIdC'] == dfExpTarget['TargetId'])\
                         & (dt['field_ID_Data'] == dfExpTarget['TargetColumnID']),
                         "inner")


        dfNewRow=dfNewRow.select(dfNewRow["ContainerObjectName"],
                                 dfNewRow["ContainerObjectPath"],
                                 dfNewRow["ControlflowName"],
                                 dfNewRow["mapId"],
                                 dfNewRow["ControlflowPath"],
                                 dfNewRow["SourceId"],
                                 dfNewRow["SourceLayerName"],
                                 dfNewRow["SourceSchema"],
                                 dfNewRow["SourceDB"],
                                 dfNewRow["SourceServer"],
                                 dfNewRow["SourceProvider"],
                                 dfNewRow["SourceTable"],
                                 dfNewRow["SourceObjectType"],
                                 dfNewRow["SourceSql"],
                                 dfNewRow["SourceConnectionKey"],
                                 dfNewRow["fromFieldName"],
                                 dfNewRow["field_ID_Data"],
                                 dfNewRow["SourceDataType"],
                                 dfNewRow["SourcePrecision"],
                                 dfNewRow["SourceScale"],

                                 dfNewRow["TargetId"],
                                 dfNewRow["TargetLayerName"],
                                 dfNewRow["TargetSchema"],
                                 dfNewRow["TargetDB"],
                                 dfNewRow["TargetServer"],
                                 dfNewRow["TargetProvider"],
                                 dfNewRow["TargetTable"],
                                 dfNewRow["TargetObjectType"],
                                 dfNewRow["TargetSql"],
                                 dfNewRow["TargetConnectionKey"],
                                 dfNewRow["TargetColumnName"],
                                 dfNewRow["TargetColumnID"],
                                 dfNewRow["TargetDataType"],
                                 dfNewRow["TargetPrecision"],
                                 dfNewRow["TargetScale"]) \
                            .withColumnRenamed("fromFieldName", "SourceColumnName") \
                            .withColumnRenamed("field_ID_Data", "SourceColumnID")
        return dfNewRow





cons=ConsumerPySpark(bootstrapServers,topics)
df=cons.flatRawMap()



    # display the dataframe (Pyspark dataframe)
   # dataframe.show(1)




# df_kafka = df_kafka.select(col("value").cast("string"))
#
#
#
# schema = StructType() \
#     .add("mapName", StringType())
#
#
# df_kafka = df_kafka.select(col("value").cast("string"))\
#     .select(from_json(col("value"), schema).alias("value"))\
#     .select("value.*")
#
#
# df_kafka.printSchema()
#
#
# df_kafka \
#     .writeStream \
#     .format("console") \
#     .start()\
#     .awaitTermination()





