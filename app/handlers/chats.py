import re

from app.handlers.constants import SEND_MESSAGE_STATE
from telegram import Update
from telegram.ext import ContextTypes

from containers.factories import get_container
from handlers.converters.chats import convert_chats_dtos_to_message
from services.web import BaseChatWebService


async def get_all_chats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    container = get_container()

    async with container() as request_container:
        service = await request_container.get(BaseChatWebService)
        chats = await service.get_all_chats()

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=convert_chats_dtos_to_message(chats=chats),
            parse_mode="MarkdownV2",
        )


async def set_chat_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    container = get_container()

    async with container() as request_container:
        service = await request_container.get(BaseChatWebService)  # type: ignore
        await service.add_listener(
            telegram_chat_id=update.effective_chat.id,
            chat_oid=context.args[0],
        )
        await context.bot.send_message(
            chat_id=update.effective_chat.id,  # type: ignore
            text='You connected to the chat.',
            parse_mode='MarkdownV2',
        )


async def start_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            'Now you are answering questions. Please choose the message and reply on it.'
            'User will see your answer on the web site.'
        ),
    )

    return SEND_MESSAGE_STATE


async def quit_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text='You quited from the dialogue with client',
        parse_mode='MarkdownV2',
    )


async def send_message_to_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message is None:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,  # type: ignore
            text='Error! Please choose the message you want to reply.',
        )
        return
    print(update.message.reply_to_message.text)
    try:
        # TODO: сделать паттерн под UUID4
        chat_oid = re.findall(r'\s{1}\(.+\)', update.message.reply_to_message.text)[0].replace(
            ' ', '',
        ).replace('(', '').replace(')', '')
    except IndexError:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,  # type: ignore
            text='Необходимо ответить именно на сообщение пользователя.',
        )
        return
    print(chat_oid)
