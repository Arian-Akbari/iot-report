import models

# Make a chat completion request using gpt-4o-mini
response = models.client.chat.completions.create(
    model="openai/gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello! How are you?"},
    ],
)

print(response.choices[0].message.content)
