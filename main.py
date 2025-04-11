import asyncio
import json
import time
import aiohttp
import logging
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

# Configuração do logging para debug
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

# Habilita CORS para todas as origens (ajuste conforme necessário)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def format_percentage(value: float) -> str:
    """Formata um valor float como percentual com duas casas decimais."""
    return f"{value:.2f}%"

def format_currency(value: float) -> str:
    """Formata um valor float para string com duas casas decimais."""
    return f"{value:.2f}"

async def fetch_campaign_insights(campaign_id: str, access_token: str, session: aiohttp.ClientSession):
    """
    Busca os insights de uma campanha individual usando o endpoint do Meta Ads.
    """
    campaign_insights_url = f"https://graph.facebook.com/v16.0/{campaign_id}/insights"
    params_campaign_insights = {
        "fields": "impressions,clicks,ctr,cpc,spend,actions",
        "date_preset": "maximum",
        "access_token": access_token
    }
    req_start = time.perf_counter()
    async with session.get(campaign_insights_url, params=params_campaign_insights) as resp:
        req_end = time.perf_counter()
        response_time = req_end - req_start
        logging.debug(f"HTTP REQUEST: GET {campaign_insights_url} completado em {response_time:.3f} segundos com status {resp.status}")
        if resp.status != 200:
            response_text = await resp.text()
            logging.error(f"Erro ao buscar insights para campanha {campaign_id}: {resp.status} - {response_text}")
            raise Exception(f"Erro {resp.status}: {response_text}")
        return await resp.json()

async def fetch_paused_campaigns(account_id: str, access_token: str):
    """
    Consulta todas as campanhas com status 'PAUSED' para uma determinada conta do Meta Ads.
    """
    timeout = aiohttp.ClientTimeout(total=3)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # URL e parâmetros para buscar campanhas pausadas
        campaigns_url = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
        filtering = json.dumps([{
            "field": "effective_status",
            "operator": "IN",
            "value": ["PAUSED"]
        }])
        params_campaigns = {
            "fields": "id,name,status",
            "filtering": filtering,
            "access_token": access_token
        }
        
        req_start = time.perf_counter()
        async with session.get(campaigns_url, params=params_campaigns) as resp:
            req_end = time.perf_counter()
            response_time = req_end - req_start
            logging.debug(f"HTTP REQUEST: GET {campaigns_url} completado em {response_time:.3f} segundos com status {resp.status}")
            if resp.status != 200:
                response_text = await resp.text()
                logging.error(f"Erro ao buscar campanhas pausadas: {resp.status} - {response_text}")
                raise Exception(f"Erro {resp.status}: {response_text}")
            campaigns_data = await resp.json()
        
        campaigns_list = campaigns_data.get("data", [])
        paused_campaigns = []
        tasks = []
        
        # Cria tarefas para buscar os insights de cada campanha pausada
        for camp in campaigns_list:
            tasks.append(fetch_campaign_insights(camp["id"], access_token, session))
        
        insights_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for idx, camp in enumerate(campaigns_list):
            campaign_obj = {
                "id": camp.get("id", ""),
                "nome_da_campanha": camp.get("name", ""),
                "cpc": "0.00",
                "impressions": 0,
                "clicks": 0,
                "ctr": "0.00%"
            }
            insight = insights_results[idx]
            if isinstance(insight, Exception):
                logging.error(f"Erro ao buscar insights para campanha {camp.get('id', '')}: {insight}")
            else:
                if "data" in insight and insight["data"]:
                    item = insight["data"][0]
                    try:
                        impressions = float(item.get("impressions", 0))
                    except Exception as e:
                        logging.error(f"Erro convertendo impressions para campanha {camp.get('id', '')}: {e}")
                        impressions = 0.0
                    try:
                        clicks = float(item.get("clicks", 0))
                    except Exception as e:
                        logging.error(f"Erro convertendo clicks para campanha {camp.get('id', '')}: {e}")
                        clicks = 0.0
                    try:
                        cpc = float(item.get("cpc", 0))
                    except Exception as e:
                        logging.error(f"Erro convertendo cpc para campanha {camp.get('id', '')}: {e}")
                        cpc = 0.0
                    
                    ctr_value = (clicks / impressions * 100) if impressions > 0 else 0.0
                    campaign_obj["impressions"] = int(impressions)
                    campaign_obj["clicks"] = int(clicks)
                    campaign_obj["ctr"] = format_percentage(ctr_value)
                    campaign_obj["cpc"] = format_currency(cpc)
            paused_campaigns.append(campaign_obj)
        
        return paused_campaigns

@app.post("/paused_campaigns")
async def get_paused_campaigns(payload: dict = Body(...)):
    """
    Endpoint para buscar campanhas pausadas do Meta Ads.
    O body da requisição deve conter 'account_id' e 'access_token'.
    """
    account_id = payload.get("account_id")
    access_token = payload.get("access_token")
    if not account_id or not access_token:
        logging.error("Payload inválido: 'account_id' ou 'access_token' ausentes.")
        raise HTTPException(status_code=400, detail="É necessário fornecer 'account_id' e 'access_token' no corpo da requisição.")
    
    try:
        paused_campaigns = await fetch_paused_campaigns(account_id, access_token)
        total_paused = len(paused_campaigns)
        result = {
            "paused_campaigns_total": total_paused,
            "paused_campaigns": paused_campaigns
        }
        logging.info(f"Campanhas pausadas retornadas: {result}")
        return result
    except Exception as e:
        logging.error(f"Erro no endpoint /paused_campaigns: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
