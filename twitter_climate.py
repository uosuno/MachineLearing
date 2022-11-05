from tweepy import Stream
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time


class TweetStreamListener(Stream):
    # on success
    def on_data(self, data):
        tweet = json.loads(data)
 
        try:
            if 'text' in tweet.keys():
                message_lst = [str(tweet['id']),
                       str(tweet['user']['name']),
                       str(tweet['user']['screen_name']),
                       str(tweet['user']['verified']),
                       str(tweet['user']['location']),
                       str(tweet['user']['id']),
                       str(tweet['user']['followers_count']),
                       tweet['text'].replace('\n',' ').replace('\r',' '),         
                       str(tweet['favorite_count']),                   
                       str(tweet['retweet_count']),                  
                       str(tweet['created_at']) 
                       ]
                       
                hash_tags_list = []
                myHashtags  = []
                media_url_list = []
                myMul = []
                htg =[]
                mul=[]
                
                ####flattenning the entities, hashtags, location details and media lists
                
                if 'hashtags' in tweet['entities']:
                
                      for hashtag in tweet['entities']['hashtags']:
                           hs = ""
                           if 'text' in hashtag:
                               hs = hashtag['text'] 
                               hash_tags_list.append(hs)
                             
                
                if len(hash_tags_list) > 0:  
                      myHashtags = ["#" + hashtag for hashtag in hash_tags_list]                
                      tweet['hashtags'] = ' '.join(myHashtags)
                      htg = [ tweet['hashtags'] ]                 
                               
                message_lst = message_lst + htg  
                
                ####################
                if 'extended_entities' in tweet.keys():
                
                      for media_u in tweet['extended_entities']['media']:
                           mu = ""
                           ur = ""
                           ty = ""
                           if 'media_url' in media_u:
                               mu = media_u['media_url'] 
                           if 'url' in media_u:
                               ur = media_u['url'] 
                           if 'type' in media_u:
                               ty = media_u['type'] 
                               
                           media_url_list.append(mu + ', '+ ur + ', '+ ty)
                
                
                
                if len(media_url_list) > 0:
                      myMul = [" " + media_u for media_u in media_url_list]
                      tweet['media_list'] = ' '.join(myMul)
                      mul = [  tweet['media_list'] ]
                             
                
                message_lst = message_lst + mul 
                
                
                #### Extract geographic location where possible
                
                pty=""
                cty=""
                ccd=""
                pfn=""
                pn=""
                if tweet['place'] != None :
                        
                               pty =  tweet['place']['place_type'] 
                       
                               cty =  tweet['place']['country']        
                        
                               ccd =  tweet['place']['country_code'] 
                       
                               pfn =  tweet['place']['full_name'] 
                        
                               pn =  tweet['place']['name'] 
                               tweet['place_type'] = pty
                               tweet['country'] = cty
                               tweet['country_code'] = ccd
                               tweet['place_full_name'] = pfn
                               tweet['place_name'] = pn      
  
                
                ###########
                
                message_lst = message_lst + [pty] + [cty] + [ccd] + [pfn] + [pn]
                
                message = '\t'.join(message_lst) 
                message = message  + '\n'
                print(message)
                firehose_client.put_record(
                    DeliveryStreamName=delivery_stream_name,
                    Record={
                        'Data': message
                    }
                )
        except (AttributeError, Exception) as e:
            print(e)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':
    # create kinesis client connection
    session = boto3.Session()
    firehose_client = session.client('firehose', region_name='ca-central-1')

    # Set kinesis data stream name
    delivery_stream_name = 'twitter_climate'

    # Set twitter credentials
    consumer_key = 'OLJlWCVJnwlBpPEMHYQ8tIS8M'
    consumer_secret = '0i634LS6hUrj2AnuUaQoWE838cta9xVVPUgmWRkaCFSDo1mqYE'
    access_token = '422219875-NjndsMrVVOhv8kwYRimCwTpQ47IPZUCBoa4RDyN7'
    access_token_secret = '2M3EY0hLn7uts3S8T1QOW1kKSNQkO7JxJBMJJwIk0ZXDC'
  

    while True:
        try:
            print('Climate Data Twitter streaming...')

            # create instance of the tweet stream listener
            stream= TweetStreamListener(consumer_key, consumer_secret,access_token, access_token_secret)

            # search twitter for the keyword
            stream.filter(track=['climate'], languages=['en'], stall_warnings=True)
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue
