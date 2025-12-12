from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from lib.sync_service import sync_contacts
from lib.notion_sync import sync_notion_to_supabase, sync_supabase_to_notion

app = FastAPI(title="Jarvis Backend")

@app.get("/")
async def root():
    return {"status": "Jarvis Backend is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/sync/contacts")
async def sync_all_contacts():
    """
    Bi-directional sync between Google Contacts and Supabase.
    """
    try:
        result = await sync_contacts()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/google-contacts")
async def sync_google_contacts_legacy():
    """
    Legacy endpoint. Redirects to /sync/contacts.
    """
    return await sync_all_contacts()

@app.post("/sync/notion-to-supabase")
async def endpoint_sync_notion_to_supabase():
    try:
        # Run synchronous function in threadpool
        return await run_in_threadpool(sync_notion_to_supabase)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/supabase-to-notion")
async def endpoint_sync_supabase_to_notion():
    try:
        # Run synchronous function in threadpool
        return await run_in_threadpool(sync_supabase_to_notion)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/all")
async def sync_everything():
    """
    Runs all syncs in order:
    1. Google -> Supabase (and Supabase -> Google)
    2. Notion -> Supabase
    3. Supabase -> Notion
    """
    results = {}
    try:
        # 1. Google <-> Supabase (Async)
        results["google_sync"] = await sync_contacts()
        
        # 2. Notion -> Supabase (Sync, run in threadpool)
        results["notion_to_supabase"] = await run_in_threadpool(sync_notion_to_supabase)
        
        # 3. Supabase -> Notion (Sync, run in threadpool)
        results["supabase_to_notion"] = await run_in_threadpool(sync_supabase_to_notion)
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
