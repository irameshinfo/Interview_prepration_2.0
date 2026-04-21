from google.cloud import secretmanager
from google.api_core.exceptions import NotFound, PermissionDenied

def get_secret(project_id, secret_id, version_id="latest"):
    try:
        client = secretmanager.SecretManagerServiceClient()

        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        response = client.access_secret_version(name=secret_name)

        secret_value = response.payload.data.decode("UTF-8")

        return secret_value

    except NotFound:
        print("❌ Secret not found")
        return None

    except PermissionDenied:
        print("❌ Permission denied. Check IAM roles")
        return None

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None


# Example usage
if __name__ == "__main__":
    project_id = "rameshgcplearning"
    secret_id = "dbpasword"

    secret = get_secret(project_id, secret_id)

    if secret:
        print(f"Secret fetched successfully...{secret}")    