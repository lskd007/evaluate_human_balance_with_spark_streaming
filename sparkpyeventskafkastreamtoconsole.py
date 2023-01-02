from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
spark = SparkSession.builder.appName('events_kafka').getOrCreate()

#TO-DO: set the spark log level to WARN
spark.sparkContext.setLogLevel('WARN')

# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
df_stedi = spark.readStream.format('kafka')\
.option('kafka.bootstrap.servers','localhost:9092')\
.option('startingOffsets','earliest')\
.option('subscribe','stedi-events')\
.load() 

# TO-DO: cast the value column in the streaming dataframe as a STRING 
df_stedi = df_stedi.selectExpr('CAST(value as string ) value')

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
redis_risk_json_schema = StructType([
    StructField('customer',StringType()),
    StructField('score',StringType()),
    StructField('riskDate',StringType())
])
df_stedi = df_stedi.withColumn('value', from_json('value', redis_risk_json_schema))
df_stedi.select('value.*').createOrReplaceTempView('CustomerRisk')

# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql('SELECT customer, score FROM CustomerRisk')

# TO-DO: sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
customerRiskStreamingDF.writeStream.outputMode('append').format('console').start().awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 