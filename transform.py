#imports
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable 
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import expr
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
#sc = SparkContext('local')

conf = SparkConf().setAppName("test").setMaster("local")
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
sqlContext = SQLContext.getOrCreate(sc)
#functions
def melt(df,id_vars, value_vars, var_name, value_name):

    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)
    
###########################################

#drop table
sqlContext.sql('drop table if exists covid_final_data')
sqlContext.sql('drop table if exists covid_final_data_2')

#testing
sqlContext.sql('drop table if exists pipeline_test_1')
test_data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
test_df = spark.createDataFrame(test_data,["v1", "v2"])
test_df.write.saveAsTable('pipeline_test_1')
#test_df.show()

sqlContext.sql('drop table if exists pipeline_test_2')
test_df.createOrReplaceTempView("pipeline_test_2_temp")
spark.sql("create table pipeline_test_2 as select * from pipeline_test_2_temp");

###########################################
# ^^^ NEW ^^^ #

#reading csv files
datac = spark.read.csv("/covid/time_series_covid19_confirmed_global.csv",header=True,inferSchema="true")
datad = spark.read.csv("/covid/time_series_covid19_deaths_global.csv",header=True,inferSchema="true")
datar = spark.read.csv("/covid/time_series_covid19_recovered_global.csv",header=True,inferSchema="true")

#extracting date columns
cols = datac.columns[4:]

#transforming data into denormalized structure
c = melt(datac,id_vars=['Province/State','Country/Region','Lat','Long'],value_vars=cols,var_name='dt',value_name='confirmed_cases')
d = melt(datad,id_vars=['Province/State','Country/Region','Lat','Long'],value_vars=cols,var_name='dt',value_name='death_cases')
r = melt(datar,id_vars=['Province/State','Country/Region','Lat','Long'],value_vars=cols,var_name='dt',value_name='recovered_cases')

#Filling in nulls in Prov/State field
c = c.withColumn('Province/State',coalesce(c['Province/State'],c['Country/Region'])) 
d = d.withColumn('Province/State',coalesce(d['Province/State'],d['Country/Region'])) 
r = r.withColumn('Province/State',coalesce(r['Province/State'],r['Country/Region'])) 

#joins into one dataframe
join1 = c.join(d,on=['Country/Region','dt','Lat','Long','Province/State'],how='left')
join2 = join1.join(r,on=['Country/Region','dt','Lat','Long','Province/State'],how='left')
join2 = join2.withColumnRenamed("Country/Region","Country").withColumnRenamed("Province/State","Prov").withColumnRenamed("Date","dt")
join3 = join2

#Date field transforamtion
func =  udf (lambda x: datetime.strptime(x, '%m/%d/%y'), DateType())
join3 = join3.withColumn('dtx', func(col('dt')))

#loading into Hive

#backup process
join3.createOrReplaceTempView("joinedDFs")
spark.sql("create table IF NOT EXISTS covid_final_data_2 as select * from joinedDFs");
#### NEW ################

join3.write.mode("overwrite").saveAsTable("covid_final_data")
join3.write.mode("overwrite").saveAsTable("covid_final_data_test2")
join3.write.mode("overwrite").saveAsTable("covid_final_data_test")
join3.write.mode("overwrite").parquet("covid_final_data.parquet")