# scp
University project for the Scalable and Cloud Computing course

## Co-Purchase Analysis: Google Dataproc Workflow

1. **Build your project locally.**
   ```sh
   ./build.sh
   ```

2. **Upload the built JAR and dataset to your GCS bucket.**
   ```sh
   ./upload_to_gcs.sh
   ```

3. **Create the Dataproc cluster with the desired number of worker nodes.**
   - For example, to create a cluster with 2 nodes:
     ```sh
     ./create_cluster.sh 2
     ```
   - For speedup experiments, repeat this step with different values (e.g., 2, 4, 8).

4. **Submit your Spark job to the Dataproc cluster.**
   ```sh
   ./run_job.sh
   ```
   - Monitor job progress and timings via the [Google Cloud Console Dataproc Jobs UI](https://console.cloud.google.com/dataproc/jobs).

5. **Delete the cluster when finished to avoid extra costs.**
   ```sh
   ./delete_cluster.sh
   ```

6. **(Repeat steps 3–5 with different node counts for speedup evaluation.)**

7. **When all experiments are finished, delete your GCS bucket to avoid storage charges.**
   ```sh
   ./delete_bucket.sh
   ```

---
**Tip:**  
- Always wait for the previous step to complete before moving to the next.
- You can view job timing and details in the Dataproc Jobs section of the Google Cloud Console.