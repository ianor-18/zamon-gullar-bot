# bot_gemini.py
import os
import json
from fastapi import FastAPI, Request
from pymongo import MongoClient
from datetime import datetime
import httpx
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

# ===== SETUP =====
MONGO_URL = os.getenv("MONGODB_URL")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_TELEGRAM_ID")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-100123456789"))

# ===== GEMINI SETUP =====
genai.configure(api_key=GOOGLE_API_KEY)
GEMINI_MODEL = "gemini-2.5-flash-lite"

client = MongoClient(MONGO_URL)
db = client["zamon_gullar"]
products_collection = db["products"]
orders_collection = db["orders"]
channel_posts_collection = db["channel_posts"]

app = FastAPI()

# ===== 1. KANAL'GA MAHSULOT POST QILISH =====
async def post_product_to_channel(product):
    caption = f"""
🌸 <b>{product['name']}</b>

{product['description']}

💰 <b>Narxi: {product['price']:,} so'm</b>

<b>TAVSIYA XIZMATLAR:</b>
- Tavsiya dori - 8,000 so'm
- Qumi/Qum - 5,000 so'm

<b>Buyurtma qilish uchun:</b>
👇 REPLY bosib "Meni buyurtma qilamani!" yozing!

#gullar #zamon #buket
"""
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    
    payload = {
        "chat_id": CHANNEL_ID,
        "photo": product['image_url'],
        "caption": caption,
        "parse_mode": "HTML",
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload)
        data = response.json()
        
        message_id = data.get('result', {}).get('message_id')
        
        channel_posts_collection.insert_one({
            "message_id": message_id,
            "product_id": str(product['_id']),
            "product_name": product['name'],
            "price": product['price'],
            "posted_date": datetime.now()
        })
        
        return message_id

# ===== 2. GEMINI AI AGENT =====
async def get_ai_response_gemini(user_message):
    try:
        products = list(products_collection.find({"active": True}))
        
        products_text = ""
        for p in products:
            products_text += f"""
🌸 {p['name']}
   Kategoriya: {p['category']}
   Narxi: {p['price']:,} so'm
   Tavsifi: {p['description']}
"""
        
        system_prompt = f"""
===== ZAMON GULLAR - AI XIZMAT BERUVCHI =====
Siz Telegram bot orqali gul do'konining AI agenti.

ISH VAQTI: 9:00-20:30 (har kuni)
TELEFON: +998 90 906 5439

📦 MAVJUD MAHSULOTLAR:
{products_text}

📋 QOIDALAR:
1. Savolga aniq javob berish
2. Tavsiya dorilarini (+8,000) so'ng
3. Buyurtma → Manzil, telefon, miqdor so'ring
4. Do'stona, emoji bilan, O'zbek tilida
5. Jami narxni aniq hisoblab berish
"""
        
        model = genai.GenerativeModel(
            model_name=GEMINI_MODEL,
            system_instruction=system_prompt
        )
        
        response = model.generate_content(
            user_message,
            generation_config={
                "temperature": 0.7,
                "max_output_tokens": 500,
            }
        )
        
        return response.text
    
    except Exception as e:
        print(f"Gemini xatosi: {str(e)}")
        return "Xatolik yuz berdi. Qayta urinib ko'ring!"

# ===== 3. WEBHOOK =====
@app.post("/webhook")
async def webhook(request: Request):
    try:
        update = await request.json()
        
        if "message" in update:
            message = update["message"]
            chat_id = message["chat"]["id"]
            text = message.get("text", "")
            user_id = message["from"]["id"]
            
            if message["chat"]["type"] == "private" and text:
                response = await get_ai_response_gemini(text)
                await send_message(chat_id, response)
                
                orders_collection.insert_one({
                    "user_id": user_id,
                    "message": text,
                    "response": response,
                    "timestamp": datetime.now()
                })
    
    except Exception as e:
        print(f"Webhook xatosi: {str(e)}")
    
    return {"ok": True}

# ===== 4. TELEGRAM XABAR YUBORISH =====
async def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    async with httpx.AsyncClient() as client:
        await client.post(url, json=payload)

# ===== 5. ADMIN API =====
@app.post("/api/add-product-and-post")
async def add_and_post_product(request: Request):
    data = await request.json()
    
    product = {
        "name": data["name"],
        "category": data["category"],
        "price": data["price"],
        "description": data["description"],
        "image_url": data["image_url"],
        "added_date": datetime.now(),
        "active": True
    }
    
    result = products_collection.insert_one(product)
    product['_id'] = result.inserted_id
    
    message_id = await post_product_to_channel(product)
    
    return {
        "status": "success",
        "product_id": str(result.inserted_id),
        "message_id": message_id,
        "channel": "@zamongullari"
    }

# ===== 6. GET ENDPOINTS =====
@app.get("/api/products")
async def get_products():
    products = list(products_collection.find({"active": True}))
    for p in products:
        p["_id"] = str(p["_id"])
    return products

@app.get("/api/orders")
async def get_orders():
    orders = list(orders_collection.find().sort("timestamp", -1).limit(50))
    for o in orders:
        o["_id"] = str(o["_id"])
    return orders

@app.get("/api/stats")
async def get_stats():
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_orders = list(orders_collection.find({"timestamp": {"$gte": today}}))
    
    return {
        "totalOrders": len(today_orders),
        "totalRevenue": len(today_orders) * 50000,
        "newCustomers": len(set([o.get("user_id") for o in today_orders]))
    }

# ===== 7. STARTUP =====
@app.on_event("startup")
async def startup():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
    payload = {"url": WEBHOOK_URL}
    async with httpx.AsyncClient() as client:
        await client.post(url, json=payload)
    print("✅ Gemini Bot Webhook sozlandi!")

@app.get("/")
async def root():
    return {"status": "🌸 Zamon Gullar Bot (Gemini) ONLINE ⚡", "model": GEMINI_MODEL}

@app.exception_handler(Exception)
async def exception_handler(request, exc):
    print(f"Global xato: {str(exc)}")
    return {"error": "Xatolik yuz berdi"}