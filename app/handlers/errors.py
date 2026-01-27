from telegram import Update
from telegram.ext import CallbackContext

from exceptions.base import ApplicationException
from exceptions.web import WebApiError


async def error_handler(update: Update, context: CallbackContext) -> None:
    try:
        raise context.error  # type: ignore
    except WebApiError as error:
        await update.effective_message.reply_text(  # type: ignore
            '\n'.join(part for part in (error.message, error.detail) if part),
        )
    except ApplicationException as error:
        print(error.meta)
        await update.effective_message.reply_text(error.message)  # type: ignore
