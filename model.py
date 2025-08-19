import os

import dotenv
import openai

dotenv.load_dotenv()

# Set your OpenRouter API key
client = openai.OpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1",
)
