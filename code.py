from google.cloud import storage

def release_temp_hold(bucket_name, blob_name, project_id):
    """Releases temporary hold on a specific GCS object."""
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name, user_project=project_id)
    blob = bucket.blob(blob_name)

    blob.reload()
    metageneration = blob.metageneration

    blob.temporary_hold = False
    blob.patch(if_metageneration_match=metageneration)

    print(f"✅ Temporary hold cleared for: {blob_name}")

# Set these values
project_id = "fit-legacy-454720-g4"
bucket_name = "dataproc-temp-us-central1-863324801108-mb0b0fdr"
blob_name = "0828ac3b-7a37-4ddf-886f-a63b5e458df8/spark-job-history/"

release_temp_hold(bucket_name, blob_name, project_id)
