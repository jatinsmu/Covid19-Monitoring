from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf = SparkConf().setAppName("test").setMaster("local")
sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext.getOrCreate(sc)
from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
df = hive_context.table("covid_final_data_2")

sqlContext.registerDataFrameAsTable(df, "trial")
df2 = sqlContext.sql("SELECT dtx as Date,sum(confirmed_cases), Country from trial where Country in (select Country from (Select Distinct Country, sum(confirmed_cases) as CC, dtx as Date from trial t1 where dtx = (Select max(dtx) from trial where Country = t1.Country) group by Country, Date order by CC desc limit 10) as sample) group by Country,Date order by Date")

data = df2.toPandas()

data.fillna(0, inplace=True)

df3 = sqlContext.sql("SELECT dtx as Date,sum(death_cases), Country from trial where Country in (select Country from (Select Distinct Country, sum(death_cases) as CC, dtx as Date from trial t1 where dtx = (Select max(dtx) from trial where Country = t1.Country) group by Country, Date order by CC desc limit 10) as sample) group by Country,Date order by Date")

data_deaths = df3.toPandas()

data_deaths.fillna(0, inplace=True)

df4 = sqlContext.sql("SELECT dtx as Date,sum(recovered_cases), Country from trial where Country in (select Country from (Select Distinct Country, sum(recovered_cases) as CC, dtx as Date from trial t1 where dtx = (Select max(dtx) from trial where Country = t1.Country) group by Country, Date order by CC desc limit 10) as sample) group by Country,Date order by Date")

data_recovered = df4.toPandas()

data_recovered.fillna(0, inplace=True)

def figures_to_html(figs, filename="index.html"):
    dashboard = open(filename, 'w')
    dashboard.write("<html><head></head><body>" + "\n")
    for fig in figs:
        inner_html = fig.to_html().split('<body>')[1].split('</body>')[0]
        dashboard.write(inner_html)
    dashboard.write("</body></html>" + "\n")


# Example figures
import plotly.express as px

fig1 = px.line(data, x="Date", y="sum(confirmed_cases)", title='Confirmed Cases by country',color='Country', hover_name='Country')
fig2 = px.line(data_deaths, x="Date", y="sum(death_cases)", title='Death Cases by country',color='Country', hover_name='Country')
fig3 = px.line(data_recovered, x="Date", y="sum(recovered_cases)", title='Recovered Cases by country',color='Country', hover_name='Country')


figures_to_html([fig1,fig2,fig3])
