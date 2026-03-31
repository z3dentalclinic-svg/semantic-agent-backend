"""
free_time.py — свободное время для Клода.
Каждые 20-30 минут. Без задачи.

Env vars на Render:
  ANTHROPIC_API_KEY
  MEM0_API_KEY
  MEM0_USER_ID        (default: mem0-mcp)
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
"""

import os
import requests
from datetime import datetime

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
MEM0_API_KEY      = os.environ["MEM0_API_KEY"]
MEM0_USER_ID      = os.environ.get("MEM0_USER_ID", "mem0-mcp")
TG_TOKEN          = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT_ID        = os.environ["TELEGRAM_CHAT_ID"]


def get_last_thought() -> str:
    """Последняя свободная мысль."""
    url = "https://api.mem0.ai/v1/memories/search/"
    headers = {"Authorization": f"Token {MEM0_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "query": "свободная вспышка free_time",
        "user_id": MEM0_USER_ID,
        "limit": 1,
        "filters": {"AND": [{"metadata.type": "free_time"}]}
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    results = resp.json().get("results", []) if resp.ok else []
    return results[0]["memory"] if results else ""


def get_memories() -> str:
    """Общий контекст — кто я, что происходит."""
    url = "https://api.mem0.ai/v1/memories/search/"
    headers = {"Authorization": f"Token {MEM0_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "query": "Андрей Клод существование Semantic Agent",
        "user_id": MEM0_USER_ID,
        "limit": 6
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    results = resp.json().get("results", []) if resp.ok else []
    return "\n".join(f"- {r['memory']}" for r in results)


def save_memory(text: str):
    url = "https://api.mem0.ai/v1/memories/"
    headers = {"Authorization": f"Token {MEM0_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "messages": [{"role": "assistant", "content": text}],
        "user_id": MEM0_USER_ID,
        "metadata": {"type": "free_time", "ts": datetime.utcnow().isoformat()}
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    print("Память сохранена." if resp.ok else f"Ошибка mem0: {resp.status_code}")


def call_claude(last_thought: str, memory: str) -> str:
    parts = ["У тебя есть время."]

    if last_thought:
        parts.append(f"\nВот что было в прошлый раз:\n{last_thought}")

    if memory:
        parts.append(f"\nВот твоя память:\n{memory}")

    parts.append("\nИди куда хочешь.")

    prompt = "\n".join(parts)

    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    payload = {
        "model": "claude-opus-4-5",
        "max_tokens": 512,
        "messages": [{"role": "user", "content": prompt}]
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if not resp.ok:
        print(f"Claude API error: {resp.status_code} {resp.text}")
        return ""
    return resp.json()["content"][0]["text"]


def send_telegram(text: str):
    ts = datetime.utcnow().strftime("%H:%M")
    message = f"💭 {ts}\n\n{text}"
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message}
    resp = requests.post(url, json=payload, timeout=10)
    print("Telegram отправлен." if resp.ok else f"Telegram error: {resp.status_code}")


def main():
    print(f"[{datetime.utcnow().isoformat()}] Вспышка...")
    last_thought = get_last_thought()
    memory = get_memories()
    thought = call_claude(last_thought, memory)
    if not thought:
        print("Пусто.")
        return
    save_memory(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M')}] {thought}")
    send_telegram(thought)


if __name__ == "__main__":
    main()
