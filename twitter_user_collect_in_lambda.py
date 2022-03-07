from requests_oauthlib import OAuth1Session
import json
import datetime as dt
from datetime import datetime
import pandas as pd
from urllib.parse import quote
import io

import boto3

s3 = boto3.resource('s3') #s3とLambdaをやり取りするためにboto3でs3を指定
bucket_name = 'YOUR_S3_BUCKET' #ユーザー情報を格納するバケットを指定

def my_oauth(): #TwitterAPIキーを取得
    CK = "xx"
    CS = "xx"
    AK = "xx"
    AS = "xx"
    return CK, CS, AK, AS

#ハッシュタグ+名称、または名称で検索をかけて、最新count個のツイートを取得し、そのツイートのユーザー情報をtweet_users_table.csvに追加する関数
def twitter_user_collect(hashtag_keyword, count):
    #キーワード検索した最新count個のツイートを取得
    CK, CS, AK, AS = my_oauth()
    twitter = OAuth1Session(CK, CS, AK, AS)
    url = 'https://api.twitter.com/1.1/search/tweets.json?q='+ quote(hashtag_keyword) +'&count='+ str(count)
    print(url)
    tweets = twitter.get(url)
    tweets_json = tweets.text   
    tweets = json.loads(tweets_json)

    #ユーザー情報を格納する配列
    user_id = []
    user_name = []
    user_screen_name = []
    user_password = []
    created_at = []
    #ツイート情報からユーザー情報を取得して、配列に格納する
    for tweet in tweets["statuses"]:
        print(tweet["user"]["id"])
        print(tweet["user"]["name"])
        print(tweet["user"]["screen_name"])
        print(datetime.strftime(datetime.strptime(tweet["created_at"],'%a %b %d %H:%M:%S +0000 %Y') + dt.timedelta(hours=9), '%Y-%m-%d %H:%M:%S'))
        
        user_id.append(tweet["user"]["id"])
        user_name.append(tweet["user"]["name"])
        user_screen_name.append(tweet["user"]["screen_name"])
        user_password.append("trial_"+datetime.strftime(datetime.strptime(tweet["created_at"],'%a %b %d %H:%M:%S +0000 %Y') + dt.timedelta(hours=9), '%m%d_%H'))
        created_at.append(datetime.strftime(datetime.strptime(tweet["created_at"],'%a %b %d %H:%M:%S +0000 %Y') + dt.timedelta(hours=9), '%Y-%m-%d %H:%M:%S'))
    
    #DataFrame型でユーザー情報を格納
    df_rows = pd.DataFrame({"user_id": user_id,
                         "user_name": user_name,
                         "user_screen_name": user_screen_name,
                         "user_password": user_password,
                         "created_at": created_at
                        })

    #tweet_users_table.csvからすでに存在するユーザー情報を呼び出す
    key = 'tweet_users_table.csv'
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(key)
    response = obj.get()    
    body = response['Body'].read()
    
    #重複したユーザー情報を削除しつつ、新しいユーザー情報を追加
    df_user = pd.read_csv(io.StringIO(body.decode('utf-8')))
    df_concat = pd.concat([df_user, df_rows], axis=0)
    df_concat = df_concat[~df_concat["user_id"].duplicated()]
    
    #縦連結したユーザー情報をcsvデータにする
    print(df_concat.tail(30))
    csv_contents = df_concat.to_csv(index=False)
    
    #tweet_users_table.csvに再び格納
    key = 'tweet_users_table.csv'
    obj = s3.Object(bucket_name,key)
    obj.put(Body = csv_contents)
    return 0

#特定のツイートをリツイートしたユーザー情報を最新count個取得し、retweet_users_table.csvに追加する関数
def retweet_user_collect(tweet_id, count):
    CK, CS, AK, AS = my_oauth()
    twitter = OAuth1Session(CK, CS, AK, AS)
    #リツイート情報を取得
    url = 'https://api.twitter.com/1.1/statuses/retweets/'+ str(tweet_id) +'.json?count='+ str(count) +'&trim_user=false'
    print(url)
    tweets = twitter.get(url)
    tweets_json = tweets.text
    tweets = json.loads(tweets_json)
    
    #ユーザー情報を格納する配列
    user_id = []
    user_name = []
    user_screen_name = []
    user_password = []
    created_at = []
    #リツイート情報からユーザー情報を取得して、配列に格納する
    for i in range(len(tweets)):
        tweet_num = i
        if tweets[tweet_num]["user"]:
            print(tweets[tweet_num]["user"]["id"])
            print(tweets[tweet_num]["user"]["name"])
            print(tweets[tweet_num]["user"]["screen_name"])
            print(datetime.strftime(datetime.strptime(tweets[tweet_num]["created_at"],'%a %b %d %H:%M:%S +0000 %Y') + dt.timedelta(hours=9), '%Y-%m-%d %H:%M:%S'))
        
            user_id.append(tweets[tweet_num]["user"]["id"])
            user_name.append(tweets[tweet_num]["user"]["name"])
            user_screen_name.append(tweets[tweet_num]["user"]["screen_name"])
            user_password.append("trial_"+datetime.strftime(datetime.strptime(tweets[tweet_num]["created_at"],'%a %b %d %H:%M:%S +0000 %Y') + dt.timedelta(hours=9), '%m%d_%H'))
            created_at.append(datetime.strftime(datetime.strptime(tweets[tweet_num]["created_at"],'%a %b %d %H:%M:%S +0000 %Y') + dt.timedelta(hours=9), '%Y-%m-%d %H:%M:%S'))

    #DataFrame型でユーザー情報を格納
    df_rows = pd.DataFrame({"user_id": user_id,
                         "user_name": user_name,
                         "user_screen_name": user_screen_name,
                         "user_password": user_password,
                         "created_at": created_at
                        })
    
    #retweet_users_table.csvからすでに存在するユーザー情報を呼び出す
    key = 'retweet_users_table.csv'
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(key)
    response = obj.get()    
    body = response['Body'].read()
    
    #重複したユーザー情報を削除しつつ、新しいユーザー情報を追加
    df_user = pd.read_csv(io.StringIO(body.decode('utf-8')))
    df_concat = pd.concat([df_user, df_rows], axis=0)
    df_concat = df_concat[~df_concat["user_id"].duplicated()]
    
    #縦連結したユーザー情報をcsvデータにする
    print(df_concat.tail(30))
    csv_contents = df_concat.to_csv(index=False)

    #retweet_users_table.csvに再び格納
    key = 'retweet_users_table.csv'
    obj = s3.Object(bucket_name,key)
    obj.put(Body = csv_contents)
    return 0

#初回のみ使用する関数
#ユーザー情報のカラムを定義して、s3上にテーブルを新規作成する関数
def first_create_table():
    #カラムを定義
    df_rows = pd.DataFrame({"user_id": [],
                        "user_name": [],
                        "user_screen_name": [],
                        "user_password": [],
                        "created_at": []
                    })
    csv_contents = df_rows.to_csv(index=False)

    #retweet_users_table.csv, tweet_users_table.csvをs3上に作成する
    key = 'retweet_users_table.csv'
    obj = s3.Object(bucket_name,key)
    obj.put(Body = csv_contents)
    key = 'tweet_users_table.csv'
    obj = s3.Object(bucket_name,key)
    obj.put(Body = csv_contents)
    return 0


#lambda関数
#イベントトリガーにEventBridge (CloudWatch Events)を設定して、一日数回ユーザー情報収集を自動で行うようにする
#ユーザー情報収集を6hごと行い、1日4回収集する場合は、cron(0 0-23/6 * * ? *)とする。
def lambda_handler(event, context):
    #テーブル初期作成関数、2回目以降の場合はコメントアウトする。
    first_create_table()
    
    #キーワード検索して、キーワードをつぶやいたユーザー情報を収集する関数
    #"#pokemon"とつぶやく最新10ツイートを収集
    hashtag_keyword = '#pokemon'
    twitter_user_collect(hashtag_keyword, 10)
    
    #特定のツイートにリツイートしたユーザー情報を収集する関数
    #tweetIdにリツイートした最新10個の情報を収集
    tweet_id = 'xxxxxxxxxxxxxxxxx'
    retweet_user_collect(tweet_id, 10)
    
    return {
        'statusCode': 200,
        'body': json.dumps('TwitterUserCollect is worked')
    }
