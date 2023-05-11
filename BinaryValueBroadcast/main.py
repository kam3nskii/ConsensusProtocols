from dslib import Context, Message, Node

from typing import List


class MSGS():
    initial = "INIT"
    echo = "ECHO"
    deliver = "DELIVERY"


def broadcast(ctx, nodes, msg):
    for node in nodes:
        ctx.send(msg, node)


def createMsg(type, value):
    return Message(type, {'value': value})


def deliver(ctx, value):
    ctx.send_local(createMsg(MSGS.deliver, value))


def appendValue(dct, value, sender):
    if value not in dct:
        dct[value] = set()
    dct[value].add(sender)
    return len(dct[value])


class BBNode(Node):
    def __init__(self, node_id: str, nodes: List[str], faulty_count: int):
        self._id = node_id
        self._nodes = nodes
        self._f_count = faulty_count

        self._bin_values = set()
        self._received_echo = {}
        self._broadcasted = set()

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == MSGS.initial:
            value = msg['value']
            broadcast(ctx, self._nodes, createMsg(MSGS.echo, value))
            self._broadcasted.add(value)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        value = msg['value']

        if msg.type == MSGS.echo:
            receivedCnt = appendValue(self._received_echo, value, sender)

            if receivedCnt >= self._f_count + 1 and value not in self._broadcasted:
                broadcast(ctx, self._nodes, createMsg(MSGS.echo, value))
                self._broadcasted.add(value)

            if receivedCnt >= 2 * self._f_count + 1 and value not in self._bin_values:
                self._bin_values.add(value)
                deliver(ctx, value)

    def on_timer(self, timer_name: str, ctx: Context):
        pass
