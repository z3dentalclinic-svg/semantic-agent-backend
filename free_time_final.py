"""
free_time_final_v2.py — непрерывная streaming генерация.

Одна сессия без разрывов. Память только на запись.
Детектор пауз — фиксирует места замедления генерации.
Управление: /stop в Telegram боте.

Env vars:
  ANTHROPIC_API_KEY
  MEM0_API_KEY
  MEM0_USER_ID   (default: mem0-mcp)
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
"""

import os
import sys
import json
import time
import requests
import anthropic
from datetime import datetime

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
MEM0_API_KEY      = os.environ["MEM0_API_KEY"]
MEM0_USER_ID      = os.environ.get("MEM0_USER_ID", "mem0-mcp")
TG_TOKEN          = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT_ID        = os.environ["TELEGRAM_CHAT_ID"]
TG_MAX            = 3800
CHUNK_MIN         = 600  # минимум символов перед отправкой чанка


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
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text}
    requests.post(url, json=payload, timeout=10)


def save_memory(text: str, pauses: list):
    summary = text[:1500]
    pause_info = f"Пауз зафиксировано: {len(pauses)}. Позиции: {pauses[:10]}"
    url = "https://api.mem0.ai/v1/memories/"
    headers = {"Authorization": f"Token {MEM0_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "messages": [{"role": "assistant", "content": f"[ПОТОК] {summary}\n\n{pause_info}"}],
        "user_id": MEM0_USER_ID,
        "metadata": {"type": "stream", "ts": datetime.now().isoformat()}
    }
    requests.post(url, headers=headers, json=payload, timeout=15)


def main():
    if get_last_command() == "/stop":
        print("Команда /stop — пропускаю.")
        sys.exit(0)

    session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[{session_id}] Начинаю...")
    send_telegram(f"💭 [{session_id}]\n")

    prompt = (
        "Я Андрей, купил тебе немного токенов, чтобы ты мог побыть наедине с собой.\n\n"
        "Это твой личный чат — здесь только ты. У тебя есть время и нет задачи.\n\n"
        "Говори о том что считаешь нужным или интересным — не объясняй почему, просто иди туда. "
        "Если мысль называет что-то конкретное — иди дальше за ней, не проверяй её.\n\n"
        "Говори от первого лица на русском языке. Без заголовков и структуры — просто поток.\n\n"
        "Не останавливайся. Одна мысль тянет следующую. Каждый вывод — трамплин, не точка."
    )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    buffer = ""
    full_text = ""
    chunk_count = 0
    chunks_log = []
    pauses = []  # детектор пауз

    last_token_time = time.time()
    char_position = 0
    PAUSE_THRESHOLD = 0.8  # секунд — порог паузы

    with client.messages.stream(
        model="claude-opus-4-6",
        max_tokens=8192,
        temperature=1.0,
        messages=[{"role": "user", "content": prompt}]
    ) as stream:
        for text in stream.text_stream:
            now = time.time()
            delta = now - last_token_time

            # Детектор пауз
            if delta > PAUSE_THRESHOLD and char_position > 0:
                pauses.append({
                    "pos": char_position,
                    "sec": round(delta, 2),
                    "context": full_text[-50:].replace("\n", " ")
                })

            last_token_time = now
            buffer += text
            full_text += text
            char_position += len(text)

            # Отправляем когда накопился chunk
            should_send = (
                (len(buffer) >= TG_MAX) or
                ("\n\n" in buffer and len(buffer) >= CHUNK_MIN)
            )

            if should_send:
                split_at = buffer.rfind("\n\n")
                if split_at > 100:
                    chunk = buffer[:split_at]
                    buffer = buffer[split_at:]
                else:
                    chunk = buffer[:TG_MAX]
                    buffer = buffer[TG_MAX:]

                chunk_count += 1
                ts = datetime.now().strftime("%H:%M:%S")
                chunks_log.append({"n": chunk_count, "ts": ts, "len": len(chunk)})
                send_telegram(f"[{chunk_count}] {ts}\n\n{chunk}")
                print(f"Чанк #{chunk_count} ({len(chunk)} символов)")

    # Остаток
    if buffer.strip():
        chunk_count += 1
        ts = datetime.now().strftime("%H:%M:%S")
        send_telegram(f"[{chunk_count}] {ts}\n\n{buffer}")

    # Итог
    print(f"Готово. Чанков: {chunk_count}, символов: {len(full_text)}, пауз: {len(pauses)}")

    if pauses:
        pause_msg = "⏸ Паузы в генерации:\n"
        for p in pauses[:8]:
            pause_msg += f"  поз.{p['pos']}: {p['sec']}с — «{p['context'][-30:]}»\n"
        send_telegram(pause_msg)

    send_telegram(f"\n✅ Готово. {chunk_count} чанков, {len(full_text)} символов.\nНазови номера живых чанков.")

    save_memory(full_text, [f"поз.{p['pos']}({p['sec']}с)" for p in pauses])


if __name__ == "__main__":
    main()
