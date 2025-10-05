from openai import OpenAI
client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="token-abc123",
)

completion = client.chat.completions.create(
    # model="Qwen/Qwen3-4B-Thinking-2507",
    model="Qwen/Qwen3-0.6B",
    messages=[
        {"role": "user", "content": "Hello, how are you?"}
    ],
    stream=True, 
)

for chunk in completion:
    print(chunk.choices[0].delta.content, end='')
    
print()