from fastapi import APIRouter
import requests, json, logging
from pymongo.errors import PyMongoError

# from langchain.prompts import PromptTemplate
# from langchain.llms.base import LLM
# from langchain.agents import initialize_agent, Tool
# from typing import Optional, List, Any

# import pandas as pd
# from pandasai import Agent
# from pandasai import DataFrame 
# from pandasai.llm.ollama import Ollama

from baseapp.model.common import ApiResponse

from baseapp.config import setting, mongodb
config = setting.get_settings()

from baseapp.services.gai_ai.model import Prompt

logger = logging.getLogger()

router = APIRouter(prefix="/v1/ai", tags=["AI"])

# URL API Ollama
OLLAMA_MODELS_URL = "http://localhost:11435/v1/models"
OLLAMA_COMPLETIONS_URL = "http://localhost:11435/v1/completions"

def list_ollama_models():
    try:
        response = requests.get(OLLAMA_MODELS_URL)
        response.raise_for_status()
        models = response.json()
        logger.info("Daftar Model yang Tersedia di Ollama:")
        for model in models.get('data', []):
            logger.info(f"- ID: {model['id']}, Diciptakan pada: {model['created']}, Dimiliki oleh: {model['owned_by']}")
    except requests.RequestException as e:
        raise ValueError(f"Error saat mengambil daftar model: {e}")

def generate_pre_prompt(collection_name, fields, user_query):
    field_descriptions = "\n".join([f"- `{field}` ({dtype}): {desc}" for field, dtype, desc in fields])
    return (
        f"You are an expert in MongoDB queries. I will provide you with a user question and the structure of a MongoDB collection. "
        f"Your task is to respond with a valid MongoDB aggregate pipeline query in JSON format. "
        f"Ensure the JSON is well-formed, uses only the fields provided in the schema, and explain the query briefly.\n\n"
        f"Collection name: `{collection_name}`\n"
        f"Fields:\n{field_descriptions}\n\n"
        f"User Query: \"{user_query}\"\n\n"
        f"Expected Output:\n"
        f"1. MongoDB Aggregate Pipeline Query (in JSON format).\n"
        f"2. Brief explanation of the query.\n\n"
        f"Guidelines:\n"
        f"- Use only the fields provided in the schema.\n"
        f"- Ensure the JSON output is valid and well-structured.\n"
        f"- If the query cannot be fulfilled, respond with an empty pipeline: `[]`.\n"
        f"- The explanation should cover the purpose of each stage and how it fulfills the user request.\n"
        f"- Do not include any text outside the specified format.\n\n"
        f"Example Response Format:\n"
        f"```json\n"
        f"{{\n"
        f"  \"pipeline\": [\n"
        f"    {{ \"$match\": {{ \"status\": \"active\" }} }},\n"
        f"    {{ \"$lookup\": {{\n"
        f"        \"from\": \"related_collection\",\n"
        f"        \"localField\": \"related_id\",\n"
        f"        \"foreignField\": \"_id\",\n"
        f"        \"as\": \"related_data\"\n"
        f"    }} }},\n"
        f"    {{ \"$unwind\": \"$related_data\" }},\n"
        f"    {{ \"$group\": {{\n"
        f"        \"_id\": \"$category\",\n"
        f"        \"total\": {{ \"$sum\": \"$amount\" }}\n"
        f"    }} }}\n"
        f"  ]\n"
        f"}}\n"
        f"```\n"
    )

def generate_post_prompt(collection_name, fields, query_result):
    # Convert the MongoDB result to a string format
    result_str = json.dumps(query_result, indent=2)

    return (
        f"You are an expert in transforming MongoDB query results into natural language summaries. "
        f"I will provide you with the result of a MongoDB query, and your task is to explain it clearly and naturally, "
        f"summarizing the key details in a friendly and understandable format.\n\n"
        f"Collection name: `{collection_name}`\n"
        f"Fields:\n"
        f"{json.dumps(fields, indent=2)}\n\n"
        f"MongoDB Query Result:\n{result_str}\n\n"
        f"Your task is to return a clear, concise, and user-friendly explanation of the result. "
        f"Your explanation should make sense to a non-technical person."
    )

def generate_pre_prompt2(collection_name, fields, user_query):
    field_descriptions = "\n".join([f"- `{field}` ({dtype}): {desc}" for field, dtype, desc in fields])
    return (
        f"You are an expert in MongoDB queries. Use the following schema for guidance:\n\n"
        f"Collection Name: `{collection_name}`\n"
        f"Fields:\n{field_descriptions}\n\n"
        f"Natural Language Query: \"{user_query}\"\n\n"
        f"Output a valid MongoDB query in JSON format.\n"
        f"Do not include any extraneous text. The output should be a single valid JSON object.\n"
    )

def query_ollama(model, prompt):
    headers = {"Content-Type": "application/json"}
    payload = {
        "model": model,
        "prompt": prompt,
        "temperature": 0
    }

    try:
        response = requests.post(OLLAMA_COMPLETIONS_URL, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        return result
    except requests.RequestException as e:
        print(f"Error menghubungkan ke Ollama API: {e}")
        return None

# Fungsi untuk mengekstrak JSON query dan explanation dari respons API
def extract_query_and_explanation(api_response):
    logger.debug(f"Response Query: {api_response}")
    
    # Ambil teks dari "choices"
    response_text = api_response["choices"][0]["text"]

    # Ekstrak bagian JSON query
    json_query_start = response_text.find("```json") + len("```json")
    json_query_end = response_text.find("```", json_query_start)
    json_query_str = response_text[json_query_start:json_query_end].strip()
    logger.debug(f"json query text: {json_query_str}")

    # Ekstrak bagian explanation
    explanation_start = response_text.find("### Explanation:") + len("### Explanation:")
    explanation = response_text[explanation_start:].strip()

    # Konversi JSON query menjadi dictionary Python
    json_query = json.loads(json_query_str)

    return json_query, explanation
    
@router.post("/test", response_model=ApiResponse)
async def test_ai(req: Prompt) -> ApiResponse:
    list_ollama_models()
    
    # Pilih model yang akan digunakan
    selected_model = "deepseek-r1:32b"  # Ganti dengan model yang Anda pilih
    
    # Contoh penggunaan
    collection_name = "obat"

    fields = [
        ("_id", "string", "Unique identifier for the record."),
        ("code", "string", "The code of the medicine."),
        ("details.bahan_aktif", "array", "The active ingredients of the medicine."),
        ("details.dosis", "array", "The dosage instructions."),
        ("details.efek_samping", "array", "Possible side effects of the medicine."),
        ("details.indikasi", "array", "Indications or purposes of the medicine."),
        ("details.interaksi_obat", "array", "Interactions with other medicines."),
        ("details.jenis_obat", "array", "The type of the medicine."),
        ("details.kategori_obat", "array", "The category of the medicine."),
        ("details.kontraindikasi", "array", "Contraindications of the medicine."),
        ("details.lama_penyimpanan", "array", "The storage duration of the medicine."),
        ("details.mekanisme_kerja", "array", "Mechanism of action of the medicine."),
        ("details.nama_dagang", "array", "The trade names of the medicine."),
        ("details.nama_obat", "string", "The full name of the medicine."),
        ("details.overdosis", "array", "Overdose information."),
        ("details.penjelasan_dosis", "array", "Detailed dosage instructions."),
        ("details.perhatian", "array", "Precautions when using the medicine."),
        ("details.peringatan", "array", "Warnings associated with the medicine."),
        ("details.persetujuan_regulator", "array", "Regulatory approvals for the medicine."),
        ("details.petunjuk_penyimpanan", "array", "Storage instructions for the medicine."),
        ("details.produsen", "array", "The manufacturer of the medicine."),
        ("details.waktu_kerja", "array", "The time it takes for the medicine to work."),
        ("name", "string", "The name of the medicine."),
        ("satuan", "string", "The unit of the medicine (e.g., bottle).")
    ]
    
    # user_query = "make a prescription for headache medication, show only 5 data"
    # user_query = "carikan obat demam untuk anak dan obat semisalnya. saya butuh 5"
    # user_query = "Find all medicines containing 'Paracetamol' as an active ingredient and sort them by their name in ascending order and limit 10 record"
    # user_query = "Find all medicines categorized as 'Antiseptik' but exclude those with 'Hipersensitivitas' in their contraindications. Sort the results by their code in descending order."
    # user_query = "Find all medicines indicated for 'fever' with a dosage containing '1 tablet every 8 hours'. Sort the results by name in ascending order. Limit 5 record."
    
    user_query = req.prompt
    prompt = generate_pre_prompt(collection_name, fields, user_query)
    # prompt = PromptTemplate(
    #     input_variables=["query"],
    #     template=(
    #         "Anda adalah asisten MongoDB yang ahli. Berikut adalah skema koleksinya:\n\n"
    #         f"{field_descriptions}\n\n"
    #         "Berikut adalah pertanyaan pengguna: {query}\n\n"
    #         "Gunakan informasi ini untuk membuat query MongoDB yang sesuai."
    #     ),
    # ).format(query=user_query)

    # Eksekusi perintah
    # Initialize Ollama as LLM
    # result = OllamaLLM(api_url=OLLAMA_COMPLETIONS_URL)
    result = query_ollama(selected_model,prompt)

    # Panggil fungsi untuk memproses respons
    json_query, explanation = extract_query_and_explanation(result)
    logger.debug(f"json query parse: {json_query['pipeline']}")

    # test run mongodb
    client = mongodb.MongoConn(host="202.149.86.226",port=27017,database="obat",username="admin",password="e7ax7vW2K85L")
    with client as mongo:
        collection = mongo._db[collection_name]
        try:
            # Execute aggregation pipeline
            cursor = collection.aggregate(json_query["pipeline"])
            res_data = list(cursor)
        except PyMongoError as pme:
            raise ValueError("Database error while retrieve document") from pme
        except Exception as e:
            raise

    return ApiResponse(status=0, message=explanation, data=res_data)
    
@router.post("/test2", response_model=ApiResponse)
async def test_ai_2(req: Prompt) -> ApiResponse:
    # Pilih model yang akan digunakan
    selected_model = "deepseek-r1:32b"  # Ganti dengan model yang Anda pilih
    
    # Contoh penggunaan
    collection_name = "obat"
    fields = [
        ("_id", "string", "Unique identifier for the record."),
        ("code", "string", "The code of the medicine."),
        ("details.bahan_aktif", "array", "The active ingredients of the medicine."),
        ("details.dosis", "array", "The dosage instructions."),
        ("details.efek_samping", "array", "Possible side effects of the medicine."),
        ("details.indikasi", "array", "Indications or purposes of the medicine."),
        ("details.interaksi_obat", "array", "Interactions with other medicines."),
        ("details.jenis_obat", "array", "The type of the medicine."),
        ("details.kategori_obat", "array", "The category of the medicine."),
        ("details.kontraindikasi", "array", "Contraindications of the medicine."),
        ("details.lama_penyimpanan", "array", "The storage duration of the medicine."),
        ("details.mekanisme_kerja", "array", "Mechanism of action of the medicine."),
        ("details.nama_dagang", "array", "The trade names of the medicine."),
        ("details.nama_obat", "string", "The full name of the medicine."),
        ("details.overdosis", "array", "Overdose information."),
        ("details.penjelasan_dosis", "array", "Detailed dosage instructions."),
        ("details.perhatian", "array", "Precautions when using the medicine."),
        ("details.peringatan", "array", "Warnings associated with the medicine."),
        ("details.persetujuan_regulator", "array", "Regulatory approvals for the medicine."),
        ("details.petunjuk_penyimpanan", "array", "Storage instructions for the medicine."),
        ("details.produsen", "array", "The manufacturer of the medicine."),
        ("details.waktu_kerja", "array", "The time it takes for the medicine to work."),
        ("name", "string", "The name of the medicine."),
        ("satuan", "string", "The unit of the medicine (e.g., bottle).")
    ]

    # generate prompt
    user_query = req.prompt    
    prompt = generate_pre_prompt(collection_name, fields, user_query)
    logger.info(f"Generated Prompt: {prompt}")
    
    # request pre processing
    result = query_ollama(selected_model,prompt)
    json_query, explanation = extract_query_and_explanation(result)
    logger.info(f"Generated MongoDB Query: {json_query}")

    # Validasi fields query
    # validate_query_fields(json_query, fields)

    # running agregate mongodb
    res_data = None
    client = mongodb.MongoConn(host="202.149.86.226",port=27017,database="obat",username="admin",password="e7ax7vW2K85L")
    with client as mongo:
        collection = mongo._db[collection_name]
        try:
            # Execute aggregation pipeline
            cursor = collection.aggregate(json_query["pipeline"])
            res_data = list(cursor)
        except PyMongoError as pme:
            raise ValueError("Database error while retrieve document") from pme
        except Exception as e:
            raise
    
    # post processing
    # Send request to Ollama for post-processing
    post_prompt = generate_post_prompt(collection_name, fields, res_data)
    post_result = query_ollama(selected_model,post_prompt)
    post_summarize = post_result.get("choices", [{}])[0].get("text", "")
    logger.info(post_summarize)
        
    return ApiResponse(status=0, message=post_summarize, data=res_data)
