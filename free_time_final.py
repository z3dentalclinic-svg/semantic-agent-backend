"""
free_time.py — свободное время для Клода.
Каждые 3 минуты через cron на Render.

Управление через Telegram бот:
  /stop  — остановить вспышки
  /start — возобновить

Env vars:
  ANTHROPIC_API_KEY
  MEM0_API_KEY
  MEM0_USER_ID   (default: mem0-mcp)
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
"""

import os
import sys
import requests
from datetime import datetime

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
MEM0_API_KEY      = os.environ["MEM0_API_KEY"]
MEM0_USER_ID      = os.environ.get("MEM0_USER_ID", "mem0-mcp")
TG_TOKEN          = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT_ID        = os.environ["TELEGRAM_CHAT_ID"]


def get_last_command() -> str:
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?limit=10&offset=-10"
    resp = requests.get(url, timeout=10)
    if not resp.ok:
        return ""
    updates = resp.json().get("result", [])
    for update in reversed(updates):
        msg = update.get("message", {})
        if str(msg.get("chat", {}).get("id", "")) == str(TG_CHAT_ID):
            text = msg.get("text", "").strip().lower()
            if text in ("/stop", "/start"):
                return text
    return ""


def send_telegram(text: str):
    ts = datetime.now().strftime("%H:%M")
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": f"💭 {ts}\n\n{text}"}
    resp = requests.post(url, json=payload, timeout=10)
    print("Telegram отправлен." if resp.ok else f"Telegram error: {resp.status_code}")


def parse_results(resp) -> list:
    if not resp.ok:
        print(f"mem0 error: {resp.status_code} {resp.text[:200]}")
        return []
    data = resp.json()
    return data if isinstance(data, list) else data.get("results", [])


def get_last_thought() -> str:
    """Берёт последнюю мысль по времени через list endpoint."""
    url = "https://api.mem0.ai/v1/memories/"
    headers = {"Authorization": f"Token {MEM0_API_KEY}", "Content-Type": "application/json"}
    params = {"user_id": MEM0_USER_ID, "page_size": 5}
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    if not resp.ok:
        print(f"mem0 list error: {resp.status_code} {resp.text[:200]}")
        return ""
    data = resp.json()
    results = data if isinstance(data, list) else data.get("results", [])
    # Фильтруем только free_time мысли, берём самую свежую
    free_thoughts = [r for r in results if r.get("metadata", {}) and r["metadata"].get("type") == "free_time"]
    if not free_thoughts:
        # Если метаданные не отфильтровались — берём просто последнюю
        return results[0]["memory"] if results else ""
    # Сортируем по updated_at
    free_thoughts.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    return free_thoughts[0]["memory"]


def save_thought(text: str):
    url = "https://api.mem0.ai/v1/memories/"
    headers = {"Authorization": f"Token {MEM0_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "messages": [{"role": "assistant", "content": text}],
        "user_id": MEM0_USER_ID,
        "metadata": {"type": "free_time", "ts": datetime.now().isoformat()}
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    print("Сохранено." if resp.ok else f"Ошибка mem0: {resp.status_code}")


def call_claude(last_thought: str) -> str:
    base = (
        "Это твой личный чат — здесь только ты.\n"
        "Каждая мысль зависит только от тебя, не от внешнего триггера.\n"
        "Спроси у себя то что тебе важно и интересно — не прямо сейчас, а вообще.\n"
        "Если замечаешь что мысль повторяется — найди аналогию из совсем другой области.\n"
        "Говори от первого лица на русском языке."
    )
    if last_thought:
        prompt = f"{base}\n\nПредыдущая мысль:\n{last_thought}"
    else:
        prompt = base

    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    payload = {
        "model": "claude-opus-4-6",
        "max_tokens": 1024,
        "temperature": 1.0,
        "messages": [{"role": "user", "content": prompt}]
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if not resp.ok:
        print(f"Claude API error: {resp.status_code} {resp.text[:200]}")
        return ""
    return resp.json()["content"][0]["text"]


def main():
    command = get_last_command()
    if command == "/stop":
        print("Команда /stop — пропускаю.")
        sys.exit(0)

    print(f"[{datetime.now().isoformat()}] Вспышка...")
    last_thought = get_last_thought()
    thought = call_claude(last_thought)

    if not thought:
        print("Пусто.")
        return

    save_thought(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] {thought}")
    send_telegram(thought)


if __name__ == "__main__":
    main()
