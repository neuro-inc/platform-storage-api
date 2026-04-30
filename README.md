# Platform Storage API

## S3/SeaweedFS/MinIO Configuration

This service supports the following S3-compatible storage backends:
- **MinIO** (for local development, testing)
- **SeaweedFS** (recommended for all production work)

### Selecting backend
Use the `s3` section in your `values.yaml` or set environment variables directly (see deployment helm templates for ENV mappings).

### Example values.yaml configuration
```yaml
# MinIO config example (commented, inactive):
# s3:
#   region: "us-east-1"
#   endpoint: "http://minio.platform:9000"
#   bucket: "platform-storage-metrics"
#   accessKeyId:
#     valueFrom:
#       secretKeyRef:
#         name: minio-secret
#         key: access_key_id
#   secretAccessKey:
#     valueFrom:
#       secretKeyRef:
#         name: minio-secret
#         key: secret_access_key

# SeaweedFS config (active):
s3:
  region: "us-east-1"
  endpoint: "http://seaweedfs-s3:9000"
  bucket: "platform-storage-metrics"
  accessKeyId:
    valueFrom:
      secretKeyRef:
        name: seaweedfs-s3-secret
        key: admin_access_key_id
  secretAccessKey:
    valueFrom:
      secretKeyRef:
        name: seaweedfs-s3-secret
        key: admin_secret_access_key
  # For readonly access: use keys read_access_key_id/read_secret_access_key.
```

### Required Kubernetes Secret for SeaweedFS
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: seaweedfs-s3-secret
  namespace: <your-namespace>
type: Opaque
data:
  admin_access_key_id: <base64-encoded>
  admin_secret_access_key: <base64-encoded>
  read_access_key_id: <base64-encoded>
  read_secret_access_key: <base64-encoded>
```

### Local Testing via aws-cli/minio/mc
1. Port-forward to the cluster S3 endpoint:
   ```shell
   kubectl port-forward svc/seaweedfs-s3 9000:9000 -n <namespace>
   ```
2. Test with AWS CLI:
   ```shell
   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... aws --endpoint-url http://localhost:9000 s3 ls
   ```
3. Or with minio-client (mc):
   ```shell
   mc alias set seaweed http://localhost:9000 <admin_access_key_id> <admin_secret_access_key>
   mc ls seaweed
   ```

### Notes
- Switch backend by editing the values.yaml (`type:` or top-level `s3:` fields)
- Readonly deployments should use `read_access_key_id` from the secret.
- See deployment Helm templates for exact ENV mapping and details.
