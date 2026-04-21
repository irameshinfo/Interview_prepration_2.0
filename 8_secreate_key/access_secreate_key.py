from google.cloud import secretmanager

def get_secret(project_id, secret_id, version_id=1):
    """
    Fetch secret value from GCP Secret Manager
    """
    client = secretmanager.SecretManagerServiceClient()

    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    response = client.access_secret_version(name=secret_name)

    secret_value = response.payload.data.decode("UTF-8")

    return secret_value


# Example usage
if __name__ == "__main__":
    project_id = "rameshgcplearning"
    secret_id = "dbpassword"

    secret = get_secret(project_id, secret_id)
    print("Secret Value:", secret)