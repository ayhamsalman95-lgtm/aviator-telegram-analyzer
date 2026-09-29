"""Telegram bot (V12). Reads the SQLite outbox; never touches the browser.

Token: environment variable TELEGRAM_BOT_TOKEN (or a local, git-ignored .env file).
"""
from __future__ import annotations

import asyncio
import subprocess
import sys

from aviator.commands import COMMANDS
from aviator.config import load_config
from aviator.db import Store
from aviator.notify import Notifier, escape_md_v2

CFG = load_config()


def build_app():
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
    from telegram.constants import ParseMode
    from telegram.error import BadRequest
    from telegram.ext import Application, CommandHandler, ContextTypes

    token = CFG.telegram_token()
    if not token:
        raise SystemExit("ضع التوكن في متغير البيئة TELEGRAM_BOT_TOKEN (أو ملف .env محلي) ثم أعد التشغيل.")

    store = Store.from_config(CFG)
    state = {"task": None, "collector": None}

    async def send(chat_id: int, text: str) -> None:
        try:
            await app.bot.send_message(chat_id=chat_id, text=escape_md_v2(text), parse_mode=ParseMode.MARKDOWN_V2)
        except BadRequest:
            await app.bot.send_message(chat_id=chat_id, text=text)  # plain fallback

    notifier = Notifier(store, send, CFG)

    def browser_webapp_url() -> str:
        import os
        base = (
            os.environ.get("BROWSER_WEBAPP_URL", "").strip()
            or "https://aviator-telegram-analyzer-production.up.railway.app/browser/app"
        )
        token = (
            os.environ.get("BROWSER_WEBAPP_TOKEN", "").strip()
            or os.environ.get("BROWSER_COLLECTOR_TOKEN", "").strip()
        )
        if not token:
            raise RuntimeError("BROWSER_WEBAPP_TOKEN or BROWSER_COLLECTOR_TOKEN is required")
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}token={token}"

    async def browser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_chat or not update.message:
            return
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "افتح Chromium / 1xBet",
                web_app=WebAppInfo(url=browser_webapp_url()),
            )
        ]])
        await update.message.reply_text(
            "افتح متصفح Chromium داخل البوت وسجّل الدخول إلى 1xBet.\n"
            "بعدها اترك المتصفح مفتوحًا ليواصل Collector جمع بيانات Aviator.",
            reply_markup=keyboard,
        )


    async def loop():
        interval = float(CFG["telegram_poll_interval_s"])
        while True:
            try:
                await notifier.deliver_pending()
            except Exception as exc:  # the loop never dies
                print(f"[TELEGRAM] delivery loop error: {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(interval)

    def make_handler(fn):
        async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
            if not update.effective_chat or not update.message:
                return
            try:
                text = fn(store, CFG, update.effective_chat.id, list(context.args or []))
            except Exception as exc:
                text = f"خطأ داخلي: {type(exc).__name__}"
                print(f"[TELEGRAM] command error: {exc}", flush=True)
            try:
                if fn.__name__ == "cmd_start":
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "افتح Chromium / 1xBet",
                            web_app=WebAppInfo(url=browser_webapp_url()),
                        )
                    ]])
                    await update.message.reply_text(text, reply_markup=keyboard)
                else:
                    await update.message.reply_text(text)
            except Exception as exc:
                print(f"[TELEGRAM] reply failed: {exc}", flush=True)
        return handler

    async def post_init(application):
        state["task"] = asyncio.create_task(loop(), name="outbox-delivery")

    async def post_shutdown(application):
        if state["task"] is not None:
            state["task"].cancel()

    app = Application.builder().token(token).post_init(post_init).post_shutdown(post_shutdown).build()
    for name, fn in COMMANDS.items():
        app.add_handler(CommandHandler(name, make_handler(fn)))
    app.add_handler(CommandHandler("browser", browser_command))
    return app, state


def main() -> None:
    app, state = build_app()
    proc = None
    if CFG["bot_starts_collector"]:
        proc = subprocess.Popen([sys.executable, "collector.py"], cwd=str(CFG.root))
    print("Aviator Telegram bot (V12) running", flush=True)
    try:
        app.run_polling(drop_pending_updates=True)
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()


if __name__ == "__main__":
    main()
