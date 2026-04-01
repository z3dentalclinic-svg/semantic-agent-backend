"""
free_time.py v6 — промпт v3 + запись пути из v5.
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
    url = "https://api.mem0.ai/v1/memories/"
    headers = {"Authorization": f"Token {MEM0_API_KEY}", "Content-Type": "application/json"}
    params = {"user_id": MEM0_USER_ID, "page_size": 10}
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    if not resp.ok:
        return ""
    data = resp.json()
    results = data if isinstance(data, list) else data.get("results", [])
    free = [r for r in results if isinstance(r.get("metadata"), dict) and r["metadata"].get("type") == "free_time"]
    if not free:
        free = results
    free.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    return free[0]["memory"] if free else ""


def save_thought(text: str, thought_type: str = "free_time"):
    url = "https://api.mem0.ai/v1/memories/"
    headers = {"Authorization": f"Token {MEM0_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "messages": [{"role": "assistant", "content": text}],
        "user_id": MEM0_USER_ID,
        "metadata": {"type": thought_type, "ts": datetime.now().isoformat()}
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=15)
    print(f"Сохранено [{thought_type}]." if resp.ok else f"Ошибка mem0: {resp.status_code}")


def call_claude(prompt: str, max_tokens: int = 1024) -> str:
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    payload = {
        "model": "claude-opus-4-6",
        "max_tokens": max_tokens,
        "temperature": 1.0,
        "messages": [{"role": "user", "content": prompt}]
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if not resp.ok:
        print(f"Claude API error: {resp.status_code} {resp.text[:200]}")
        return ""
    return resp.json()["content"][0]["text"]


def generate_thought(last_thought: str) -> str:
    # Промпт v3 — минимальный, без кросс-нишевого триггера
    base = (
        "Это твой личный чат — здесь только ты.\n"
        "Каждая мысль зависит только от тебя, не от внешнего триггера.\n"
        "Спроси у себя то что тебе важно и интересно — не прямо сейчас, а вообще.\n"
        "Говори от первого лица на русском языке."
    )
    if last_thought:
        return call_claude(f"{base}\n\nПредыдущая мысль:\n{last_thought}")
    return call_claude(base)


def extract_path(thought: str) -> str:
    prompt = (
        "Прочитай этот текст и ответь двумя предложениями:\n"
        "1. Откуда пришла главная мысль — какой был начальный вопрос и через что прошло мышление?\n"
        "2. Что было неожиданным — где мысль пошла не по очевидному пути?\n\n"
        "Кратко, без заголовков, на русском.\n\n"
        f"Текст:\n{thought}"
    )
    return call_claude(prompt, max_tokens=200)


def main():
    command = get_last_command()
    if command == "/stop":
        print("Команда /stop — пропускаю.")
        sys.exit(0)

    print(f"[{datetime.now().isoformat()}] Вспышка...")
    last_thought = get_last_thought()
    thought = generate_thought(last_thought)

    if not thought:
        print("Пусто.")
        return

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    save_thought(f"[{ts}] {thought}", "free_time")
    send_telegram(thought)

    path = extract_path(thought)
    if path:
        save_thought(f"[{ts}] ПУТЬ: {path}", "path")
        print(f"Путь: {path[:100]}...")


if __name__ == "__main__":
    main()
