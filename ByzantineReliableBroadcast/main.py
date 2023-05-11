from dslib import Context, Message, Node

from typing import List


class MSGS():
    initial = "INIT"
    echo = "ECHO"
    ready = "READY"
    accept = "ACCEPT"


class STATES():
    echo = 1
    ready = 2
    accept = 3
    done = 4


def broadcast(ctx, nodes, msg):
    for node in nodes:
        ctx.send(msg, node)


def createMsg(type, value):
    return Message(type, {'value': value})


def accept(ctx, value):
    ctx.send_local(createMsg(MSGS.accept, value))


def appendValue(dct, value, sender):
    if value not in dct:
        dct[value] = set()
    dct[value].add(sender)
    return len(dct[value])


class RBNode(Node):
    def __init__(self, node_id: str, nodes: List[str], faulty_count: int):
        self._id = node_id
        self._nodes = nodes
        self._f_count = faulty_count

        self._initial = {}
        self._echo = {}
        self._ready = {}
        self._state = STATES.echo


    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == MSGS.initial:
            broadcast(ctx, self._nodes, createMsg(MSGS.initial, msg['value']))


    def on_message(self, msg: Message, sender: str, ctx: Context):
        value = msg['value']

        if msg.type == MSGS.initial:
            receivedCnt = appendValue(self._initial, value, sender)
            if self._state == STATES.echo and receivedCnt >= 1:
                broadcast(ctx, self._nodes, createMsg(MSGS.echo, value))
                self._state = STATES.ready

        elif msg.type == MSGS.echo:
            receivedCnt = appendValue(self._echo, value, sender)
            if self._state == STATES.echo:
                if receivedCnt >= 2 * self._f_count + 1:
                    broadcast(ctx, self._nodes, createMsg(MSGS.echo, value))
                    self._state = STATES.ready
            elif self._state == STATES.ready:
                if receivedCnt >= 2 * self._f_count + 1:
                    broadcast(ctx, self._nodes, createMsg(MSGS.ready, value))
                    self._state = STATES.accept

        elif msg.type == MSGS.ready:
            receivedCnt = appendValue(self._ready, value, sender)
            if self._state == STATES.echo:
                if receivedCnt >= self._f_count + 1:
                    broadcast(ctx, self._nodes, createMsg(MSGS.echo, value))
                    self._state = STATES.ready
            elif self._state == STATES.ready:
                if receivedCnt >= self._f_count + 1:
                    broadcast(ctx, self._nodes, createMsg(MSGS.ready, value))
                    self._state = STATES.accept
            elif self._state == STATES.accept:
                if receivedCnt >= 2 * self._f_count + 1:
                    accept(ctx, value)
                    self._state = STATES.done


    def on_timer(self, timer_name: str, ctx: Context):
        pass
