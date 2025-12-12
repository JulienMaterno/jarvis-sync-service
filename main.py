from fastapi import FastAPI

app = FastAPI(title="Jarvis Backend")

@app.get("/")
async def root():
    return {"status": "Jarvis Backend is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/sync/google-contacts")
async def sync_google_contacts():
    return {"message": "not implemented"}

@app.post("/sync/supabase-to-notion")
async def sync_supabase_to_notion():
    return {"message": "not implemented"}
