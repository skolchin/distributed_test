from openai import OpenAI
client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="token-abc123",
)

completion = client.chat.completions.create(
    model="Qwen/Qwen3-0.6B",
    messages=[
        {"role": "user", "content": "Hello, how are you?"}
    ]
)

for line in (completion.choices[0].message.content or '').splitlines():
    print(line)