# Deployment Guide: Google Cloud Run

This guide explains how to deploy the Jarvis Backend to Google Cloud Run and set up scheduled triggers.

## Prerequisites

1.  **Google Cloud Project**: Ensure you have a GCP project.
2.  **gcloud CLI**: Installed and authenticated (`gcloud auth login`).
3.  **APIs Enabled**:
    *   Cloud Run API
    *   Cloud Build API
    *   Cloud Scheduler API
    *   Google People API (already done)

## Step 1: Create a GCS Bucket for Backups

Since Cloud Run is ephemeral, we need a place to store backups.

```bash
# Replace [YOUR_BUCKET_NAME] with a unique name (e.g., jarvis-backups-123)
gcloud storage buckets create gs://[YOUR_BUCKET_NAME] --location=us-central1
```

## Step 2: Deploy to Cloud Run

Run the following command to deploy. You will need to provide your environment variables.

```bash
gcloud run deploy jarvis-backend \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars SUPABASE_URL="[YOUR_URL]",SUPABASE_KEY="[YOUR_KEY]",NOTION_TOKEN="[YOUR_TOKEN]",NOTION_DATABASE_ID="[YOUR_DB_ID]",GOOGLE_CLIENT_ID="[YOUR_ID]",GOOGLE_CLIENT_SECRET="[YOUR_SECRET]",GOOGLE_REFRESH_TOKEN="[YOUR_TOKEN]",GCS_BACKUP_BUCKET="[YOUR_BUCKET_NAME]"
```

*Note: For better security, use `--no-allow-unauthenticated` and set up a service account for the scheduler, but for simplicity, we start with unauthenticated access if you want to trigger it easily.*

## Step 3: Set up Scheduled Triggers

### 1. Sync Every Hour

```bash
gcloud scheduler jobs create http jarvis-sync-hourly \
  --schedule "0 * * * *" \
  --uri "https://[YOUR-CLOUD-RUN-URL]/sync/all" \
  --http-method POST \
  --time-zone "UTC"
```

### 2. Backup Once a Week (e.g., Sunday at 2 AM)

```bash
gcloud scheduler jobs create http jarvis-backup-weekly \
  --schedule "0 2 * * 0" \
  --uri "https://[YOUR-CLOUD-RUN-URL]/backup" \
  --http-method POST \
  --time-zone "UTC"
```

## Future Modules

The system is designed to be modular.
- **Tasks**: Endpoint `/sync/tasks` is ready (currently a placeholder).
- **Mail**: Endpoint `/sync/mail` is ready (currently a placeholder).

To implement these, simply add the logic in `lib/` and update the endpoints in `main.py`.
