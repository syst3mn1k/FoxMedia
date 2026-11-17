import asyncio

import httpx
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from app.core.config import settings
from app.core.logging import configure_logging


router = Router(name="foxmedia_bot")


def is_authorized(message: Message) -> bool:
    return (
        message.from_user is not None
        and message.from_user.id in settings.telegram_allowed_user_ids
    )


@router.message(CommandStart())
async def start_handler(message: Message) -> None:
    if not is_authorized(message):
        await message.answer("Access denied.")
        return

    await message.answer(
        "FoxMedia Butler is online.\n"
        "Use /health to check system status."
    )


@router.message(Command("help"))
async def help_handler(message: Message) -> None:
    if not is_authorized(message):
        await message.answer("Access denied.")
        return

    await message.answer(
        "/start — Start the bot\n"
        "/health — Check Butler API status\n"
        "/help — Show available commands"
    )


@router.message(Command("health"))
async def health_handler(
    message: Message,
    api_client: httpx.AsyncClient,
) -> None:
    if not is_authorized(message):
        await message.answer("Access denied.")
        return

    try:
        response = await api_client.get("/health/ready")
        data = response.json()

    except (httpx.RequestError, ValueError):
        await message.answer(
            "FoxMedia Butler API: unavailable"
        )
        return

    status_text = (
        "READY"
        if response.status_code == 200
        else "NOT READY"
    )

    dependencies = data.get("dependencies", {})

    lines = [
        f"FoxMedia Butler API: {status_text}",
        f"PostgreSQL: {dependencies.get('postgresql', 'unknown')}",
        f"Redis: {dependencies.get('redis', 'unknown')}",
    ]

    await message.answer("\n".join(lines))


async def main() -> None:
    configure_logging(settings.log_level)

    if settings.telegram_bot_token is None:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

    if not settings.telegram_allowed_user_ids:
        raise RuntimeError("Telegram allowlist is empty")

    bot = Bot(
        token=settings.telegram_bot_token.get_secret_value()
    )

    dispatcher = Dispatcher()
    dispatcher.include_router(router)

    async with httpx.AsyncClient(
        base_url=settings.butler_api_url.rstrip("/") + "/",
        timeout=5.0,
    ) as api_client:
        await dispatcher.start_polling(
            bot,
            api_client=api_client,
        )


if __name__ == "__main__":
    asyncio.run(main())
