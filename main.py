import asyncio
import json
import time
import aiohttp
import logging
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

# Configuração detalhada do logging para debug
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
    logging.debug(f"Formatando valor {value} como porcentagem")
    formatted = f"{value:.2f}%"
    logging.debug(f"Valor formatado: {formatted}")
    return formatted

def format_currency(value: float) -> str:
    logging.debug(f"Formatando valor {value} como moeda")
    formatted = f"{value:.2f}"
    logging.debug(f"Valor formatado: {formatted}")
    return formatted

async def fetch_campaign_insights(campaign_id: str, access_token: str, session: aiohttp.ClientSession):
    """
    Busca os insights de uma campanha individual usando o endpoint do Meta Ads.
    """
    logging.debug(f"[fetch_campaign_insights] INÍCIO para campanha_id: {campaign_id}")
    campaign_insights_url = f"https://graph.facebook.com/v16.0/{campaign_id}/insights"
    params_campaign_insights = {
        "fields": "impressions,clicks,ctr,cpc,spend,actions",
        "date_preset": "maximum",
        "access_token": access_token
    }
    logging.debug(f"[fetch_campaign_insights] URL: {campaign_insights_url}")
    logging.debug(f"[fetch_campaign_insights] Params: {params_campaign_insights}")
    
    req_start = time.perf_counter()
    async with session.get(campaign_insights_url, params=params_campaign_insights) as resp:
        req_end = time.perf_counter()
        response_time = req_end - req_start
        logging.debug(f"[fetch_campaign_insights] Requisição concluída em {response_time:.3f} segundos com status {resp.status}")
        
        try:
            response_text = await resp.text()
            logging.debug(f"[fetch_campaign_insights] Resposta texto: {response_text}")
        except Exception as e:
            logging.error(f"[fetch_campaign_insights] Erro ao ler resposta texto para campanha {campaign_id}: {e}")
            response_text = ""
            
        if resp.status != 200:
            logging.error(f"[fetch_campaign_insights] Erro HTTP {resp.status} para campanha {campaign_id}. Resposta: {response_text}")
            raise Exception(f"Erro {resp.status}: {response_text}")
        
        try:
            response_json = await resp.json()
            logging.debug(f"[fetch_campaign_insights] JSON decodificado: {response_json}")
        except Exception as e:
            logging.error(f"[fetch_campaign_insights] Erro ao decodificar JSON para campanha {campaign_id}: {e}")
            raise
        
        logging.debug(f"[fetch_campaign_insights] FINALIZANDO para campanha_id: {campaign_id}")
        return response_json

async def fetch_paused_campaigns(account_id: str, access_token: str):
    """
    Consulta todas as campanhas com status 'PAUSED' para uma determinada conta do Meta Ads.
    """
    logging.debug(f"[fetch_paused_campaigns] INÍCIO - account_id: {account_id}, access_token: {access_token}")
    timeout = aiohttp.ClientTimeout(total=3)
    async with aiohttp.ClientSession(timeout=timeout) as session:
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
        
        logging.debug(f"[fetch_paused_campaigns] Buscando campanhas pausadas com URL: {campaigns_url}")
        logging.debug(f"[fetch_paused_campaigns] Params campanhas: {params_campaigns}")
        req_start = time.perf_counter()
        async with session.get(campaigns_url, params=params_campaigns) as resp:
            req_end = time.perf_counter()
            response_time = req_end - req_start
            logging.debug(f"[fetch_paused_campaigns] Requisição de campanhas pausadas completada em {response_time:.3f} segundos com status {resp.status}")
            
            try:
                response_text = await resp.text()
                logging.debug(f"[fetch_paused_campaigns] Resposta texto campanhas: {response_text}")
            except Exception as e:
                logging.error(f"[fetch_paused_campaigns] Erro ao ler resposta de campanhas pausadas: {e}")
                response_text = ""
                
            if resp.status != 200:
                logging.error(f"[fetch_paused_campaigns] Erro ao buscar campanhas pausadas: status {resp.status} - {response_text}")
                raise Exception(f"Erro {resp.status}: {response_text}")
            
            try:
                campaigns_data = await resp.json()
                logging.debug(f"[fetch_paused_campaigns] JSON recebido de campanhas pausadas: {campaigns_data}")
            except Exception as e:
                logging.error(f"[fetch_paused_campaigns] Erro ao decodificar JSON de campanhas pausadas: {e}")
                raise
        
        campaigns_list = campaigns_data.get("data", [])
        logging.debug(f"[fetch_paused_campaigns] Número de campanhas pausadas encontradas: {len(campaigns_list)}")
        paused_campaigns = []
        tasks = []
        
        for camp in campaigns_list:
            logging.debug(f"[fetch_paused_campaigns] Agendando insights para campanha: {camp}")
            tasks.append(fetch_campaign_insights(camp["id"], access_token, session))
        
        logging.debug(f"[fetch_paused_campaigns] Iniciando asyncio.gather para insights de {len(tasks)} campanhas")
        insights_results = await asyncio.gather(*tasks, return_exceptions=True)
        logging.debug(f"[fetch_paused_campaigns] Resultado do gather: {insights_results}")
        
        for idx, camp in enumerate(campaigns_list):
            logging.debug(f"[fetch_paused_campaigns] Processando dados para campanha index {idx} - ID: {camp.get('id', '')}")
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
                logging.error(f"[fetch_paused_campaigns] Exceção nos insights para campanha {camp.get('id', '')}: {insight}")
            else:
                if "data" in insight and insight["data"]:
                    item = insight["data"][0]
                    logging.debug(f"[fetch_paused_campaigns] Insights brutos para campanha {camp.get('id', '')}: {item}")
                    try:
                        impressions = float(item.get("impressions", 0))
                        logging.debug(f"[fetch_paused_campaigns] Impressions convertidas: {impressions}")
                    except Exception as e:
                        logging.error(f"[fetch_paused_campaigns] Erro convertendo impressions para campanha {camp.get('id', '')}: {e}")
                        impressions = 0.0
                    try:
                        clicks = float(item.get("clicks", 0))
                        logging.debug(f"[fetch_paused_campaigns] Clicks convertidos: {clicks}")
                    except Exception as e:
                        logging.error(f"[fetch_paused_campaigns] Erro convertendo clicks para campanha {camp.get('id', '')}: {e}")
                        clicks = 0.0
                    try:
                        cpc = float(item.get("cpc", 0))
                        logging.debug(f"[fetch_paused_campaigns] CPC convertido: {cpc}")
                    except Exception as e:
                        logging.error(f"[fetch_paused_campaigns] Erro convertendo cpc para campanha {camp.get('id', '')}: {e}")
                        cpc = 0.0
                    
                    ctr_value = (clicks / impressions * 100) if impressions > 0 else 0.0
                    logging.debug(f"[fetch_paused_campaigns] CTR calculado: {ctr_value}")
                    
                    campaign_obj["impressions"] = int(impressions)
                    campaign_obj["clicks"] = int(clicks)
                    campaign_obj["ctr"] = format_percentage(ctr_value)
                    campaign_obj["cpc"] = format_currency(cpc)
                    logging.debug(f"[fetch_paused_campaigns] Objeto da campanha atualizado: {campaign_obj}")
                else:
                    logging.debug(f"[fetch_paused_campaigns] Sem dados de insights para a campanha {camp.get('id', '')}")
            paused_campaigns.append(campaign_obj)
        
        logging.debug(f"[fetch_paused_campaigns] FINALIZANDO. Total de campanhas pausadas processadas: {len(paused_campaigns)}")
        return paused_campaigns

@app.post("/paused_campaigns")
async def get_paused_campaigns(payload: dict = Body(...)):
    """
    Endpoint para buscar campanhas pausadas do Meta Ads.
    O body da requisição deve conter 'account_id' e 'access_token'.
    """
    logging.debug("[endpoint: /paused_campaigns] Requisição recebida com payload:")
    logging.debug(json.dumps(payload, indent=2, ensure_ascii=False))
    
    account_id = payload.get("account_id")
    access_token = payload.get("access_token")
    
    if not account_id or not access_token:
        logging.error("[endpoint: /paused_campaigns] Payload inválido: 'account_id' ou 'access_token' ausentes.")
        raise HTTPException(status_code=400, detail="É necessário fornecer 'account_id' e 'access_token' no corpo da requisição.")
    
    logging.debug(f"[endpoint: /paused_campaigns] Valores extraídos: account_id={account_id}, access_token={access_token[:10]}...")  # Parciais para segurança
    
    try:
        logging.debug("[endpoint: /paused_campaigns] Chamando fetch_paused_campaigns()")
        paused_campaigns = await fetch_paused_campaigns(account_id, access_token)
        total_paused = len(paused_campaigns)
        result = {
            "paused_campaigns_total": total_paused,
            "paused_campaigns": paused_campaigns
        }
        logging.info(f"[endpoint: /paused_campaigns] Resposta final: {json.dumps(result, indent=2, ensure_ascii=False)}")
        return result
    except Exception as e:
        logging.error("[endpoint: /paused_campaigns] Erro ao processar a requisição", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    logging.info("Iniciando aplicação com uvicorn na porta 8000")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
