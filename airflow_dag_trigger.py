#!/usr/bin/env python3
import argparse
import json
import requests
import sys
import time
from datetime import datetime
from requests.auth import HTTPBasicAuth

class AirflowCLI:
    def __init__(self, airflow_url, keycloak_url, realm, client_id, client_secret, username, password):
        self.airflow_url = airflow_url.rstrip("/")
        self.username = username
        self.password = password
        self.keycloak_url = keycloak_url.rstrip("/")
        self.realm = realm
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }

    def authenticate(self):
        token_url = f"{self.keycloak_url}/realms/{self.realm}/protocol/openid-connect/token"
        payload = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password
        }
        try:
            response = requests.post(token_url, data=payload)
            if response.status_code != 200:
                print(f"Failed to get token: HTTP {response.status_code}")
                print(response.text)
                sys.exit(1)
            data = response.json()
            self.access_token = data.get("access_token")
            if not self.access_token:
                print("Failed to retrieve access token from Keycloak response.")
                sys.exit(1)
            print("Authentication successful. Access token obtained.\n")
            return self.access_token
        except Exception as e:
            print(f"Error during Keycloak authentication: {e}")
            sys.exit(1)

    def trigger_dag(self, dag_id, conf=None):
        trigger_url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        payload = {"conf": conf or {}}
        print(f"Triggering DAG '{dag_id}'...")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        response = requests.post(trigger_url, headers=headers, json=payload)
    
        if response.status_code in (200, 201):
            data = response.json()
            dag_run_id = data.get("dag_run_id", "N/A")
            state = data.get("state", "QUEUED")
            print(f"DAG Run ID: {dag_run_id}")
            print(f"Status: {state}")
        else:
            print(f"Failed to trigger DAG: HTTP {response.status_code}")
            print(response.text)
            sys.exit(1)

def get_dag_status(self, dag_id):
    dag_runs_url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date"
    spinner = ['⠋','⠙','⠹','⠸','⠼','⠴','⠦','⠧','⠇','⠏']
    spinner_idx = 0
    start_time = datetime.now()
    print(f"Fetching latest DAG run status for '{dag_id}'...")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {self.access_token}"
    }

    try:
        response = requests.get(dag_runs_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve DAG status: HTTP {response.status_code}")
            print(response.text)
            sys.exit(1)
        data = response.json()
        if not data.get("dag_runs"):
            print("No DAG runs found for this DAG.")
            return
        latest_run = data["dag_runs"][0]
        dag_run_id = latest_run["dag_run_id"]
        state = latest_run["state"]
        execution_date = latest_run["execution_date"]

        for _ in range(10):
            sys.stdout.write(f"\r{spinner[spinner_idx % len(spinner)]} State: {state}")
            sys.stdout.flush()
            spinner_idx += 1
            time.sleep(0.3)

        elapsed = (datetime.now() - start_time).seconds
        print("\r" + " " * 50 + "\r")  # Clear spinner line
        print(f"DAG Run ID: {dag_run_id}")
        print(f"Execution Date: {execution_date}")
        print(f"Final Status: {state} (elapsed: {elapsed}s)")
    except Exception as e:
        print(f"Error fetching DAG status: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Trigger or check status of an Airflow DAG.")
    parser.add_argument("--dag", required=True, help="DAG ID to operate on")
    parser.add_argument("--airflow-url", default="https://airflow.rapid.nx1cloud.com", help="Base URL of Airflow instance")
    parser.add_argument("--conf", help="JSON string of DAG run configuration", default="{}")
    parser.add_argument("--username", required=True, help="Airflow username")
    parser.add_argument("--password", required=True, help="Airflow password")
    parser.add_argument("--keycloak-url", required=True, help="Base URL of Keycloak")
    parser.add_argument("--realm", required=True, help="Keycloak realm")
    parser.add_argument("--client-id", required=True, help="Keycloak client ID for Airflow")
    parser.add_argument("--client-secret", required=True, help="Keycloak client secret for Airflow")

    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument("--trigger", action="store_true", help="Trigger the specified DAG")
    action_group.add_argument("--get-status", action="store_true", help="Get the status of the latest DAG run")

    args = parser.parse_args()
    try:
        conf_dict = json.loads(args.conf)
    except json.JSONDecodeError:
        print("Invalid JSON provided for --conf argument.")
        sys.exit(1)

    cli = AirflowCLI(
        airflow_url=args.airflow_url,
        username=args.username,
        password=args.password,
        keycloak_url=args.keycloak_url,
        realm=args.realm,
        client_id=args.client_id,
        client_secret=args.client_secret
    )
    cli.authenticate()
    if args.trigger:
        cli.trigger_dag(args.dag, conf_dict)
    elif args.get_status:
        cli.get_dag_status(args.dag)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()