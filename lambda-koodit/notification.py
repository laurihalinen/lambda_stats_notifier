import json
import boto3


sns = boto3.client('sns')
sns_topic_arn = 'arn:aws:sns:us-east-1:590183795888:TopPlayersTopic'  # Replace with your SNS Topic ARN

def lambda_handler(event, context):
   
    if 'top_players' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing top_players data in event')
        }

    top_players = event['top_players']  
    
   
    top_players_str = "Top 3 Players:\n\n"
    for i, player in enumerate(top_players, start=1):
        top_players_str += f"{i}. {player['playerName']}\n"
        top_players_str += f"   Wins: {player['wins']}, Losses: {player['losses']}, Win Rate: {player['win_rate']}, KD Ratio: {player['kd_ratio']}\n\n"

    
    sns.publish(
        TopicArn=sns_topic_arn,
        Subject='Top 3 Players Notification',
        Message=top_players_str
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Top players notification sent successfully!')
    }
