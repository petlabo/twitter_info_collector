# twitter_info_collector
How to collect tweet and user information

## プロダクト概要
AWSサービスLambdaを用いて、一日数回ツイッターのユーザー情報を収集し、csvファイルとしてユーザー情報を積み増していくシステムを組んだ。\
本システムを用いることで、特定のハッシュタグ(またはキーワード)をつぶやいたユーザーと、特定のツイートをリツイートしたユーザーを収集できる。
### 本プロダクトを用いることでできること
・リツイート、ハッシュタグキャンペーン\
・特定のツイートをつぶやいたユーザーへと、リツイートしたユーザーから有向ブラフを設定する\
・ハッシュタグ(またはキーワード)をつぶやいたユーザー情報からクラスタを作成する

## プロダクト設計
次に本プロダクトのシステム設計を示す。

<img src="https://user-images.githubusercontent.com/92337825/157008573-3add83a6-2ddb-4763-bfac-4aa317d239c6.png" width="800">

1. EventBridgeのCRONスケジュールによってLambdaのイベントトリガーが発生
2. lambda関数でcsvを呼び出し
3. TwitterAPIでユーザー情報を取得
4. ユーザー情報をcsvに追加してS3に再び格納

## プロダクトのソースコード・必要手順
本プロダクトを再現するために必要となるAWS上の手順、およびLambda関数のソースコードを示す。

### Lambda関数ソースコード
<a href="https://github.com/petlabo/twitter_info_collector">twitter_user_collect_in_lambda.py</a>\
ただし、このソースコードを用いるためには、pandas, requests-oauthライブラリが必要である。\
・サイドバー->レイヤー->レイヤーを作成 ローカルPC上でインストールしたライブラリをzipコマンドで圧縮したうえでアップロード、作成したレイヤーのARNをコピー\
・Lambda画面 レイヤーを追加->ARNを入力

### 1. EventBridgeのCRONスケジュールによってLambdaのイベントトリガーが発生
・Lambda画面上で、トリガーの追加->EventBridgeを選択\
・スケジュール式を次のように入力 6時間ごとの収集の場合、 cron(0 0-23/6 \* \* ? \*) 

### 2., 4. ユーザー情報をcsvから取り出し、収納する
・Lambda関数からS3にアクセスする権限が必要\
Lambda画面 設定->アクセス権限->ロール にて、現在のロールが表示されるので、S3の対象テーブルへのアクセスが許可されたロールに変更

### 3. TwitterAPIでユーザー情報を取得
・TwitterAPIを叩くためには、TwitterDeveloperにてTwitterAPIの申請をする必要がある
