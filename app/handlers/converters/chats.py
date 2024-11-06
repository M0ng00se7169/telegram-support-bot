from dtos.messages import ChatListItemDTO


def convert_chats_dtos_to_message(chats: list[ChatListItemDTO]) -> str:
    return "\n".join(
        (
            "List of all available chats:",
            "\n".join(
                rf"ChatOID: `{chat.oid}`\. Problem: {chat.title}" for chat in chats  # type: ignore
            ),
        ),
    )
