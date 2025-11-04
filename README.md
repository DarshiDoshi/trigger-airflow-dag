# trigger-airflow-dag
CLI to trigger and get the status of the Airflow DAGs.

## Example Usages:
### Trigger DAG
```
python airflow_dag_trigger.py \
    --airflow-url <airflow_url> \
    --keycloak-url <user_keycloak_url> \
    --realm <realm_name> \
    --client-id <client_id> \
    --client-secret <client_secret> \
    --username <user_mail-id> \
    --password <user_password> \
    --dag <dag_name> \
    --trigger
```
### Get DAG status
```
python airflow_dag_trigger.py \
    --airflow-url <airflow_url> \
    --keycloak-url <user_keycloak_url> \
    --realm <realm_name> \
    --client-id <client_id> \
    --client-secret <client_secret> \
    --username <user_mail-id> \
    --password <user_password> \
    --dag <dag_name> \
    --get-status
```
