
import json
from typing import Optional
from pyspark.sql import DataFrame, Column, functions as F, session, streaming, window

spark = session.SparkSession.builder.gerOrCreate()
sc = spark.sparkContext
event_encrypt = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt

def decode_udf(col: Optional[Column], enc='utf-8') -> Column: 
    def decode_byte(a_byte: Optional[bytearray]): 
        if a_byte is not None: 
            try: return bytes(a_byte).decode(enc)
            except: pass
    return F.udf(decode_byte)(col)


#%% Read the Stream. 

conn_string = 'on-hold'
event_name  = 'on-hold'
delta_path = '/mnt/container/.../webhook/delta'
check_path = '/mnt/container/.../webhook/checkpoints'


#%% Write the stream. 

read_confs = {
    'eventhubs.startingposition' : json.dumps({
        'offset' : '-1', 'seqNo' : -1, 
        'enqueuedTime' : None, 
        'isInclusive'  : True }), 
    'eventhubs.connectionString' : event_encrypt(conn_string)}

raw_stream = spark.readStream.format('eventhubs').options(**read_confs).load()

stream_meta = raw_stream.select(
    F.lit(event_name).alias('source'), 
    'body', decode_udf('body').alias('json_body'), 
    F.current_timestamp().aliast('ingest_ts'), 
    F.current_timestamp().cast('date').alias('p_ingest_date') )

stream_table = ( stream_meta.writeStream.format('delta')
    .outputMode('append').queryName('event_raw_to_bronze')  # 'overwrite'
    .partitionBy('p_ingest_date')
    .options(**{
        'checkpointLocation': check_path, 'mergeSchema': False}))

stream_table.start(delta_path)
    