import openai
from config.config import config


def sentiment_analysis(comment) -> str:
    if comment:
        openai.api_key = config['openai']['api_key']
        completion = openai.ChatCompletion.create(
            model='gpt-3.5-turbo',
            messages=[
                {
                    "role": "system",
                    "content": f"You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL. You are to respond with one word from the option specified above, do not add anything else. Here is the comment: {comment}"
                }
            ]
        )
        return completion.choices[0].message['content']
    return "Empty"


if __name__ == "__main__":
    sample_comment = "This is a test comment."
    result = sentiment_analysis(sample_comment)
    print(f'Sentiment analysis result: {result}')
