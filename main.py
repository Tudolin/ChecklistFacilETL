import json
import os
import time
from datetime import datetime, timedelta

import requests
from google.cloud import storage


def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

def fetch_evaluation_data(evaluation_id, api_key):
    """Realiza a extração dos dados relevantes da API.

    Args:
        evaluation_id (_type_): Id da avaliação a ser buscada.
        api_key (_type_): Chave de API.

    Returns:
        _type_: JSON com dados brutos da avaliação.
    """
    base_url = f"https://integration.checklistfacil.com.br/v2/evaluations/{evaluation_id}"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept-Language": "pt-br"
    }

    for attempt in range(5):
        response = requests.get(base_url, headers=headers)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print(f"Erro 429 - Too Many Attempts. Aguardando 1 minuto para tentar novamente...")
            time.sleep(60)
        else:
            print(f"Erro: {response.status_code} - {response.content}")
            return None

    print("Número máximo de tentativas excedido.")
    return None

def fetch_evaluations(api_key, base_url):
    """Extrai todos os ID de avaliação durante o período selecionado.

    Args:
        api_key (_type_): Chave de API
        base_url (_type_): Endpoint da API

    Returns:
        _type_: Lista com as avaliações brutas.
    """

    today = datetime.now().date()
    two_days_ago = today - timedelta(days=2)
    two_days_ago_formatted = two_days_ago.isoformat()
    today_formatted = today.isoformat()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    all_evaluations = []
    page = 1

    while True:
        params = {
            "page": page,
            "concludedAt[gte]": f"{two_days_ago_formatted}T00:00:01+00:00",
            "concludedAt[lte]": f"{today_formatted}T23:59:59+00:00",
        }

        for attempt in range(5):
            response = requests.get(base_url, headers=headers, params=params)

            if response.status_code == 200:
                break
            elif response.status_code == 429:
                print(f"Erro 429 - Too Many Attempts. Aguardando 1 minuto para tentar novamente...")
                time.sleep(60)
            else:
                print(f"Error: {response.status_code} - {response.content}")
                return None

        if response.status_code != 200:
            print(f"Erro: {response.status_code} - {response.content}")
            return None

        data = response.json()

        if not data or 'data' not in data or len(data['data']) == 0:
            break

        all_evaluations.extend(data['data'])
        page += 1

        if not data['meta'].get('hasMore'):
            break

    return all_evaluations

def process_data(data):
    """Filtra apenas os evaluationID cujo status é 6, 5 que seriam os Aprovados/Concluidos

    Args:
        data (_type_): lista de avaliações brutas.

    Returns:
        _type_: Lista de evaluationsID.
    """
    evaluations_id = []
    for item in data:
        if item['status'] not in (6, 5):
            continue
        evaluation_id = item['evaluationId']
        evaluations_id.append(evaluation_id)
    return evaluations_id

def carregar_nao_conformidades_incidencias():
    """Carrega o banco de dados com as não conformidades padrão avaliadas.

    Returns:
        _type_: lista de não conformidades.
    """
    with open('nao-conformidades.json', 'r', encoding='utf-8') as f:
        nao_conformidades_data = json.load(f)
    nao_conformidades_lista = [item['nao conformidades'] for item in nao_conformidades_data]
    return set(nao_conformidades_lista)

def extrair_nao_conformidades(data, nao_conformidades_conhecidas):
    """Recebe os dados e percorre toda a resposta verificando as não conformidades e adicionando em uma lista separada, fazendo tratamento de exceções e validando as respostas.

    Args:
        data (_type_): Dados obtidos como resposta da API.
        nao_conformidades_conhecidas (_type_): Lista de não conformidades definidas.

    Returns:
        _type_: Uma lista contendo as não conformidades encontradas na avaliação.
    """
    nao_conformidades = []

    item_map = {item['id']: item for category in data['categories'] for item in category['items']}

    for category in data['categories']:
        area = category['name']
        for item in category['items']:
            answer = item.get('answer', {})
            evaluative = answer.get('evaluative')
            selected_options = answer.get('selectedOptions', [])
            comment = item.get('comment', '')

            if evaluative in [1, 7]:
                if selected_options:
                    for option in selected_options:
                        nao_conformidade = option['text']
                        if nao_conformidade in nao_conformidades_conhecidas and nao_conformidade not in ["Outro", "Outros"]:
                            nao_conformidades.append((nao_conformidade, area, comment))
                elif comment:
                    nao_conformidade = comment
                    nao_conformidades.append((nao_conformidade, area, comment))

            for dependency in item.get('dependencies', []):
                if dependency.get('answer') == evaluative:
                    for unlocked in dependency.get('unlocks', []):
                        unlocked_item_id = unlocked['id']
                        unlocked_item = item_map.get(unlocked_item_id)

                        if unlocked_item:
                            unlocked_answer = unlocked_item.get('answer', {})
                            unlocked_selected_options = unlocked_answer.get('selectedOptions', [])
                            unlocked_comment = unlocked_item.get('comment', '')

                            if unlocked_selected_options:
                                for option in unlocked_selected_options:
                                    nao_conformidade = option['text']
                                    if nao_conformidade in nao_conformidades_conhecidas and nao_conformidade not in ["Outro", "Outros"]:
                                        nao_conformidades.append((nao_conformidade, area, unlocked_comment))
                            elif unlocked_comment:
                                nao_conformidade = unlocked_comment
                                nao_conformidades.append((nao_conformidade, area, unlocked_comment))

    return nao_conformidades

def verify_history(evaluation_id, api_key):
    """Verifica o histórico de comentários da avaliação.
    """
    url = 'https://api-analytics.checklistfacil.com.br/v1/evaluations/history'

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    params = {
        'evaluationId': evaluation_id
    }
    
    response = requests.get(
        url,
        headers=headers,
        params=params
    )

    if response.status_code != 200:
        print(f"Erro: {response.status_code} - {response.content}")
        return None

    return response.json()

def data_to_jsonl(response_data, nao_conformidades):
    """Realiza o "Salvamento" dos dados em JSONL no padrão determinado abaixo.

    Args:
        response_data (_type_): Dados da avaliação.
        nao_conformidades (_type_): Lista de não conformidades da avaliação.

    Returns:
        _type_: Jsonl tratado e formatado pronto para ser salvo no seu bucket.
    """
    non_compliance_list = []

    for nc in nao_conformidades:
        non_compliance = {
            "nonCompliance": nc[0],
            "area": nc[1],
            "comment": nc[2],
        }
        non_compliance_list.append(non_compliance)

    record = {
        "evaluationId": response_data.get('id'),
        "status": response_data.get('status'),
        "score": response_data.get('score'),
        "startedAt": response_data.get('startedAt'),
        "concludedAt": response_data.get('concludedAt'),
        "checklist": {
            "id": response_data['checklist'].get('id'),
            "name": response_data['checklist'].get('name')
        },
        "unit": {
            "id": response_data['unit'].get('id'),
            "name": response_data['unit'].get('name')
        },
        "non_compliances": non_compliance_list
    }

    jsonl_data = json.dumps(record, ensure_ascii=False)

    return jsonl_data


def save_to_bucket_jsonl(data, bucket_name, file_path):
    """Realiza o salvamento do seu arquivo no caminho do bucket determinado.

    Args:
        data (_type_): Arquivo JSONL.
        bucket_name (_type_): Nome do seu bucket.
        file_path (_type_): Caminho onde sera salvo o arquivo.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob.upload_from_string(data)
    print(f"Data saved to gs://{bucket_name}/{file_path}")


def main(event, context):
    """Start point da ETL que orquestra toda a pipeline, onde você deve chamar para executar a operação.

    Args:
        event (_type_): Argumento da requisição no cloud Functions.
        context (_type_): Argumento da requisição no cloud Functions.

    Returns:
        _type_: Status de conclusão da execução
    """
    base_url = "https://api-analytics.checklistfacil.com.br/v1/evaluations"
    bucket_name = "NOME-DO-SEU-BUCKET-PRINCIPAL"

    try:
        with open('secrets.json') as token_file:
            token = json.load(token_file)
            api_key = token["api-key"]
    except FileNotFoundError:
        print("Error: secrets.json file not found.")
        return "Error: secrets.json file not found.", 500
    except json.JSONDecodeError:
        print("Error: Invalid JSON in secrets.json file.")
        return "Error: Invalid JSON in secrets.json file.", 500

    evaluations_data = fetch_evaluations(api_key, base_url)
    if evaluations_data:
        evaluation_ids = process_data(evaluations_data)

        nao_conformidades_conhecidas = carregar_nao_conformidades_incidencias()

        for evaluation_id in evaluation_ids:
            response_data = fetch_evaluation_data(evaluation_id, api_key)
            if not response_data:
                continue

            history = verify_history(evaluation_id, api_key)
            if history:
                # Caso queira mandar algumas avaliações especificas para um bucket distindo, exemplo, as avaliações que foram re-abertas com o comentário "Erro", as salvando em um bucket a parte para uma análise mais aprofundada.
                for item in history['data']:
                    if item['comment'] == 'Erro':
                        iat_bucket_name = 'BUCKET-ERRO'
                        output_file = f"reports/{response_data['unit']['id']}/{response_data['startedAt'][:10]}_{evaluation_id}.jsonl"
                        jsonl_data = data_to_jsonl(response_data, [])
                        save_to_bucket_jsonl(jsonl_data, iat_bucket_name, output_file)
                        break

            nao_conformidades = extrair_nao_conformidades(response_data, nao_conformidades_conhecidas)
            
            output_file = f"reports/{response_data['unit']['id']}/{response_data['startedAt'][:10]}_{evaluation_id}.jsonl"
            jsonl_data = data_to_jsonl(response_data, nao_conformidades)
            save_to_bucket_jsonl(jsonl_data, bucket_name, output_file)

    return 'Completed'


# Para testar localmente, apenas executar o main passando None nos parametros :D.
# main(None, None)