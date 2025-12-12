from fastapi import FastAPI, HTTPException
from lib.sync_service import sync_google_contacts_to_supabase

app = FastAPI(title="Jarvis Backend")

@app.get("/")
async def root():
    return {"status": "Jarvis Backend is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/sync/google-contacts")
async def sync_google_contacts():
    try:
        result = await sync_google_contacts_to_supabase()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/supabase-to-notion")
async def sync_supabase_to_notion():
    return {"message": "not implemented"}
