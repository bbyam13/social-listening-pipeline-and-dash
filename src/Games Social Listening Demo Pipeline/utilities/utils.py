from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

categories = [
    'gameplay_mechanics', 'matchmaking_game_balance', 'game_performance', 
    'replayability', 'character', 'monetization', 'bugs_glitches_techissues',
    'graphics_audio', 'suggestion_feedback', 'account_issues', 
    'cheating_hacking', 'toxicity', 'player_retention', 'onboarding'
]

# Provide general context of the content and goals of sentiment extraction
prompt_directions = "Tell me if this user generated content is about the game overall or specific to gameplay mechanics, music, script, character development, or something else" 

def build_sentiment_schema(categories):
    # Build all sentiment categories
    category_structs = []
    for cat in categories:
        category_structs.append(f"{cat}: STRUCT<sentiment: STRING, sub_topic: STRING>")
    
    # Combine: fixed fields + dynamic categories
    schema_parts = [
        "overall: STRING",
        "summary: STRING"
    ] + category_structs + [
        "other: ARRAY<STRUCT<topic: STRING, sentiment: STRING>>"
    ]
    
    return f"STRUCT<{', '.join(schema_parts)}>"


def build_prompt(categories):
    # Build the JSON format section dynamically
    category_json_parts = []
    for cat in categories:
        category_json_parts.append(f'"{cat}": {{"sentiment": <sentiment if present else null>, "sub_topic": <sub category of topic>}}')
    
    #directions to LLM to specify format and topics
    prompt_format_and_categories = f'''Be concise using only these topics as the key and the value as a json dictionary with the sentiment and sub category of the topic. Senitment value can only be positive, negative, or neutral. Return JSON ONLY. No markdown formats or tick marks. No other text outside the JSON. JSON format:
        {{"overall": <sentiment if present else null>,
            "summary": <10 word or less concise summary of key points that affected sentiment>,
            {',\n            '.join(category_json_parts)},
            "other": [{{"topic": <topic>, "sentiment": <sentiment (positive, negative, or neutral)>}}]
        }}'''
    
    return prompt_directions + prompt_format_and_categories

