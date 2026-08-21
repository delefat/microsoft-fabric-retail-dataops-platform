import os,argparse,requests
from azure.identity import ClientSecretCredential
from fabric_cicd import FabricWorkspace,publish_all_items

def workspace_id(name,token):
    r=requests.get('https://api.fabric.microsoft.com/v1/workspaces',headers={'Authorization':f'Bearer {token.token}'},timeout=60)
    r.raise_for_status()
    for w in r.json().get('value',[]):
        if w.get('displayName')==name: return w['id']
    raise ValueError(f'Workspace not found: {name}')

p=argparse.ArgumentParser()
p.add_argument('--aztenantid',required=True);p.add_argument('--azclientid',required=True);p.add_argument('--azspsecret',required=True)
p.add_argument('--target_env',required=True);p.add_argument('--items_in_scope',required=True)
a=p.parse_args()
cred=ClientSecretCredential(tenant_id=a.aztenantid,client_id=a.azclientid,client_secret=a.azspsecret)
token=cred.get_token('https://api.fabric.microsoft.com/.default')
env=a.target_env.lower(); ws_name=os.environ[f'{env}WorkspaceName'.upper()]
items=[x.strip().strip('"').strip("'") for x in a.items_in_scope.strip('[]').split(',') if x.strip()]
ws=FabricWorkspace(workspace_id=workspace_id(ws_name,token),environment=env,
    repository_directory=os.environ.get('GITDIRECTORY','fabric'),item_type_in_scope=items,token_credential=cred)
publish_all_items(ws)
print(f'Deployed to {ws_name}')
