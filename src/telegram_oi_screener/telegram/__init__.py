"""Telegram integration package.

Import `telegram_oi_screener.telegram.bot.TelegramScreenerBot` directly when the
runtime should start the Telegram polling layer. Keeping package import light
avoids a scheduler/bot circular import.
"""
