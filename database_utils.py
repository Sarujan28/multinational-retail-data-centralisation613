import yaml

class DatabaseConnecter:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            credentials = yaml.safe_load(file)
        self.credentials = credentials
        
        return credentials