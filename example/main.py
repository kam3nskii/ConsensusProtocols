from dslib import Context, Message, Node


class MSGS():
    req = "REQUEST"
    res = "RESPONSE"


class Server(Node):
    def __init__(self, node_id: str):
        self._id = node_id

    def on_local_message(self, msg: Message, ctx: Context):
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == MSGS.req:
            pong = Message(MSGS.res, {'ans': '42'})
            ctx.send(pong, sender)

    def on_timer(self, timer_name: str, ctx: Context):
        pass


class Client(Node):
    def __init__(self, node_id: str, server_id: str):
        self._id = node_id
        self._server_id = server_id

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == MSGS.req:
            ctx.send(msg, self._server_id)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == MSGS.res:
            ctx.send_local(msg)

    def on_timer(self, timer_name: str, ctx: Context):
        pass
