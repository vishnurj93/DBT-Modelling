import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

keyVaultName = os.environ["KEY_VAULT_NAME"]
KVUri = f"https://{keyVaultName}.vault.azure.net"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)

inputs = ['snowflake_username', 'snowflake_password', 'snowflake_account']
keys = []

for item in inputs:
    secretName = input(f'Provide the {item}')
    keys.append(secretName)

for secretName, secretValue in zip(inputs, keys):
    try:
        print(f"Creating a secret in {keyVaultName} called '{secretName}' with the value '{secretValue}' ...")
        client.set_secret(secretName, secretValue)
    except:
        print('Could not authorize!!!\n Exiting the program now.')