from pyparsing import col
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import types as T
import pyspark.sql.functions as F

class Autoflatten:

    def __init__(self, df):
        self.df = df

    def flatten(self):
        complex_fields = dict([
            (field.name, field.dataType)
            for field in self.df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])

        qualify = list(complex_fields.keys())[0] + "_"

        while len(complex_fields) != 0:
            col_name = list(complex_fields.keys())[0]

            if isinstance(complex_fields[col_name], T.StructType):
                expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k)
                            for k in [n.name for n in complex_fields[col_name]]
                            ]

                self.df = self.df.select("*", *expanded).drop(col_name)

            elif isinstance(complex_fields[col_name], T.ArrayType):
                self.df = self.df.withColumn(col_name, F.explode(col_name))

            complex_fields = dict([
                (field.name, field.dataType)
                for field in self.df.schema.fields
                if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
            ])

        for df_col_name in self.df.columns:
            self.df = self.df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))

        return self.df

    def flattenX(self):
        # compute Complex Fields (Lists and Structs) in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in self.df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
        while len(complex_fields) != 0:
            col_name = list(complex_fields.keys())[0]
            print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

            # if StructType then convert all sub element to columns.
            # i.e. flatten structs
            if (type(complex_fields[col_name]) == StructType):
                expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                            [n.name for n in complex_fields[col_name]]]
                self.df = self.df.select("*", *expanded).drop(col_name)

            # if ArrayType then add the Array Elements as Rows using the explode function
            # i.e. explode Arrays
            elif (type(complex_fields[col_name]) == ArrayType):
                self.df = self.df.withColumn(col_name, explode_outer(col_name))

            # recompute remaining Complex Fields in Schema
            complex_fields = dict([(field.name, field.dataType)
                                   for field in self.df.schema.fields
                                   if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
        return self.df