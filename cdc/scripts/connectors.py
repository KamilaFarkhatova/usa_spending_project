"""Применить шаблон коннектора или показать его состояние без вывода секретов."""

import argparse
import json
import os
import re
from pathlib import Path
from urllib.parse import quote

import requests


ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = {
    "source": ROOT / "cdc" / "connectors" / "recipients-source.json.template",
    "sink": ROOT / "cdc" / "connectors" / "sink.json.template",
}


def connector_name(kind, login):
    if kind == "source":
        return f"recipients-source-{login}"
    return f"sink-{login}"


def session():
    base_url = os.environ.get("CONNECT_URL", "").rstrip("/")
    if not base_url:
        raise ValueError("Укажите CONNECT_URL адреса Kafka Connect REST API.")
    user = os.environ.get("CONNECT_USER")
    password = os.environ.get("CONNECT_PASSWORD")
    if bool(user) != bool(password):
        raise ValueError("CONNECT_USER и CONNECT_PASSWORD задаются вместе.")
    client = requests.Session()
    if user:
        client.auth = (user, password)
    client.headers.update({"Accept": "application/json"})
    return client, base_url


def render_template(kind, login):
    path = TEMPLATES[kind]
    template = path.read_text(encoding="utf-8")
    names = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", template))
    missing = sorted(name for name in names if not os.environ.get(name))
    if missing:
        raise ValueError("Не заданы переменные: " + ", ".join(missing))

    rendered = re.sub(
        r"\$\{([A-Z][A-Z0-9_]*)\}",
        lambda match: json.dumps(os.environ[match.group(1)])[1:-1],
        template,
    )
    config = json.loads(rendered)
    expected_name = connector_name(kind, login)
    if config.get("name") != expected_name:
        raise ValueError(f"В шаблоне ожидалось имя {expected_name}.")
    return config


def apply(kind, login):
    document = render_template(kind, login)
    client, base_url = session()
    name = document["name"]
    encoded_name = quote(name, safe="")
    detail_url = f"{base_url}/connectors/{encoded_name}"
    response = client.get(detail_url, timeout=20)

    if response.status_code == 404:
        response = client.post(
            f"{base_url}/connectors", json=document, timeout=30
        )
        operation = "created"
    elif response.ok:
        response = client.put(
            f"{detail_url}/config", json=document["config"], timeout=30
        )
        operation = "updated"
    else:
        raise RuntimeError(
            f"Kafka Connect не ответил на проверку коннектора: HTTP {response.status_code}."
        )

    if not response.ok:
        raise RuntimeError(
            f"Kafka Connect не применил конфигурацию: HTTP {response.status_code}."
        )
    print(f"Коннектор {name}: конфигурация {operation}; секреты не выводились.")


def show_status(kind, login):
    client, base_url = session()
    name = connector_name(kind, login)
    response = client.get(
        f"{base_url}/connectors/{quote(name, safe='')}/status", timeout=20
    )
    if not response.ok:
        raise RuntimeError(
            f"Не удалось получить состояние {name}: HTTP {response.status_code}."
        )
    result = response.json()
    task_states = [task.get("state", "UNKNOWN") for task in result.get("tasks", [])]
    print(
        f"{name}: connector={result.get('connector', {}).get('state', 'UNKNOWN')}; "
        f"tasks={task_states}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("apply", "status"))
    parser.add_argument("connector", choices=("source", "sink"))
    args = parser.parse_args()

    login = os.environ.get("PROJECT_LOGIN", "")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", login):
        parser.error("Укажите корректный PROJECT_LOGIN.")

    try:
        if args.action == "apply":
            apply(args.connector, login)
        else:
            show_status(args.connector, login)
    except Exception as error:
        parser.exit(1, f"Ошибка Kafka Connect: {error}\n")


if __name__ == "__main__":
    main()
