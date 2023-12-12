from time import sleep

import pyspark
import torch
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from config.config import config

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

sid = SentimentIntensityAnalyzer()


def sentiment_analysis(comment) -> str:
    if comment:
        # Get sentiment scores
        scores = sid.polarity_scores(comment)

        # Determine sentiment based on compound score
        compound_score = scores['compound']
        if compound_score >= 0.05:
            return 'positive'
        elif compound_score <= -0.05:
            return 'negative'
        else:
            return 'neutral'

    return "Empty"




# model_name = "textattack/distilbert-base-uncased-SST-2"
# tokenizer = AutoTokenizer.from_pretrained(model_name)
# model = AutoModelForSequenceClassification.from_pretrained(model_name)


# def sentiment_analysis(comment) -> str:
#     if comment:
#         # Tokenize input and run through the PyTorch model
#         inputs = tokenizer(comment, return_tensors="pt", truncation=True)
#         outputs = model(**inputs)
#         logits = outputs.logits
#
#         # Predict sentiment based on the logits
#         predicted_class = torch.argmax(logits, dim=1).item()
#
#         # Assuming the model outputs a label map, adjust this accordingly
#         label_map = {0: 'negative', 1: 'neutral', 2: 'positive'}
#         result = label_map.get(predicted_class, 'unknown')
#
#         return result
#
#     return "Empty"


def start_streaming(spark):
    topic = 'customers_review'
    while True:
        try:
            stream_df = (spark.readStream.format("socket")
                         .option("host", "0.0.0.0")
                         .option("port", 9999)
                         .load()
                         )

            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])

            stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))

            sentiment_analysis_udf = udf(sentiment_analysis, StringType())

            stream_df = stream_df.withColumn('feedback',
                                             when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                                             .otherwise(None)
                                             )

            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

            query = (kafka_df.writeStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                     .option("kafka.security.protocol", config['kafka']['security.protocol'])
                     .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanisms'])
                     .option('kafka.sasl.jaas.config',
                             'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                             'password="{password}";'.format(
                                 username=config['kafka']['sasl.username'],
                                 password=config['kafka']['sasl.password']
                             ))
                     .option('checkpointLocation', '/tmp/checkpoint')
                     .option('topic', topic)
                     .start()
                     .awaitTermination()
                     )

        except Exception as e:
            print(f'Exception encountered: {e}. Retrying in 10 seconds')
            sleep(10)


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    start_streaming(spark_conn)
