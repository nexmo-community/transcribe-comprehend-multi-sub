import sys
import boto3
import json

t = sys.argv[1]		# text
l = sys.argv[2]    	# language code, e.g. en
r = sys.argv[3]		# region, e.g. us-east-1

comprehend = boto3.client(service_name='comprehend', region_name=r)

# print (comprehend.detect_sentiment(Text=t, LanguageCode=l))
print (json.dumps(comprehend.detect_sentiment(Text=t, LanguageCode=l)))
