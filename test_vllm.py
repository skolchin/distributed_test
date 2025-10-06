import click
import httpx
import readline
from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam
from typing import List

@click.command()
@click.option('-m', '--model', help='Full name of model to use')
def main(model: str):

    # Query for models available 
    response = httpx.get('http://localhost:8080/v1/models')
    response.raise_for_status()
    if not 'data' in (js := response.json()) or not js['data']:
        print('No models deployed, exiting')
        exit(1)

    model_names = [j['id'] for j in js['data']]
    if model and model not in model_names:
        print(f'Models {model} is deployed, exiting')
        exit(1)

    if not model and len(js['data']) > 1:
        while True:
            print('More than one model deployed, choose one:')
            for n,m in enumerate(model_names):
                print(f'  {n}: {m}')
            try:
                sel = int(input('Enter model number: '))
                assert (sel > 0 and sel < len(model_names))
                model = model_names[sel]
                break
            except KeyboardInterrupt:
                return
            except:
                print('Wrong choice, try again')
    elif not model:
        model = model_names[0]

    client = OpenAI(
        base_url="http://localhost:8080/v1",
        api_key="token-abc123",
    )

    print(f'Model selected: {model}')
    messages: List[ChatCompletionMessageParam] = [{
        "role": "system",
        "content": "You are a helpful AI assistant."
    }]
    
    while True:
        try:
            q = input('Enter your question: ')
            if not q: continue

            messages.append(
                {"role": "user", "content": q}
            )

            completion = client.chat.completions.create(
                model=model,
                messages=messages,
                stream=True, 
            )

            response = ''
            for chunk in completion:
                content = chunk.choices[0].delta.content or ''
                print(content, end='')
                response += chunk.choices[0].delta.content or ''
            print()

            messages.append(
                {"role": "assistant", "content": response}
            )

        except KeyboardInterrupt:
            print()
            break

if __name__ == '__main__':
    main()