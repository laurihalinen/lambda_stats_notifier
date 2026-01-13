import json
import boto3
import csv
import io
from decimal import Decimal
from operator import itemgetter

# Initialize clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
table = dynamodb.Table('PlayerStats')  # Replace with your DynamoDB table name
sns_topic_arn = 'arn:aws:sns:us-east-1:590183795888:TopPlayersTopic'  # Replace with your SNS Topic ARN

def lambda_handler(event, context):
    
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']
    
    
    s3 = boto3.client('s3')
    
    
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    file_content = response['Body'].read().decode('utf-8')
    
   
    csv_reader = csv.DictReader(io.StringIO(file_content))
    
    players = []
    
    for row in csv_reader:
        
        player_name = row['playerName']
        
        
        kills = Decimal(row['kills'])
        deaths = Decimal(row['deaths'])
        matches = Decimal(row['matches'])
        wins = Decimal(row['wins'])
        losses = Decimal(row['losses'])

        
        kd_ratio = Decimal(row['kd_ratio'])
        win_rate = Decimal(row['win_rate'])

        
        players.append({
            'playerName': player_name,
            'kills': kills,
            'deaths': deaths,
            'matches': matches,
            'wins': wins,
            'losses': losses,
            'kd_ratio': kd_ratio,
            'win_rate': win_rate
        })

       
        table.put_item(
            Item={
                'playerName': player_name,
                'kills': kills,
                'deaths': deaths,
                'matches': matches,
                'wins': wins,
                'losses': losses,
                'kd_ratio': kd_ratio,  
                'win_rate': win_rate   
            }
        )

    sorted_players = sorted(players, key=itemgetter('win_rate'), reverse=True)

  
    top_3_players = sorted_players[:3]

   
    top_players_str = "Top 3 Players:\n\n"
    for i, player in enumerate(top_3_players, start=1):
        top_players_str += f"{i}. {player['playerName']}\n"
        top_players_str += f"   Wins: {player['wins']}, Losses: {player['losses']}, Win Rate: {player['win_rate']}, KD Ratio: {player['kd_ratio']}\n\n"
    
   
    sns.publish(
        TopicArn=sns_topic_arn,
        Subject='Top 3 Players Notification',
        Message=top_players_str
    )

    return {
        'statusCode': 200,
        'body': json.dumps('CSV processed and top players notified successfully!')
    }
