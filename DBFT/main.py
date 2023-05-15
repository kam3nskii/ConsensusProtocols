from dslib import Context, Message, Node

from typing import List


class RB_MSGS():
    initial = "RB_INIT"
    echo = "RB_ECHO"
    ready = "RB_READY"
    accept = "RB_ACCEPT"


class MSGS():
    init = "INIT"


class RB_STATES():
    echo = 1
    ready = 2
    accept = 3
    done = 4


def broadcast(ctx, nodes, msg):
    for node in nodes:
        ctx.send(msg, node)


def rb_create_msg(type, rb_val):
    value, sender = rb_val
    return Message(type, {'value': value, 'sender': sender})


class DBFT(Node):
    def __init__(self, node_id: str, nodes: List[str], faulty_count: int):
        self._id = node_id
        self._nodes = nodes
        self._f_count = faulty_count

        self._rb_states = {}
        self._rb_initial_vals = {}
        self._rb_echo_vals = {}
        self._rb_ready_vals = {}

        self._proposals = [None] * len(self._nodes)

    def _rb_note_value(self, storage, value, sender):
        if value not in self._rb_states:
            self._rb_states[value] = RB_STATES.echo
        if value not in storage:
            storage[value] = set()
        storage[value].add(sender)
        return len(storage[value])

    def _rb_send_echo(self, ctx, rb_val):
        broadcast(ctx, self._nodes, rb_create_msg(RB_MSGS.echo, rb_val))
        self._rb_states[rb_val] = RB_STATES.ready

    def _rb_send_ready(self, ctx, rb_val):
        broadcast(ctx, self._nodes, rb_create_msg(RB_MSGS.ready, rb_val))
        self._rb_states[rb_val] = RB_STATES.accept

    def _rb_accept(self, rb_val):
        self._rb_states[rb_val] = RB_STATES.done
        value, sender = rb_val
        sender = int(sender)
        self._proposals[int(sender)] = value
        print(f'{self._id} RB-delivered from {sender}: {self._proposals}')

    def on_local_message(self, msg: Message, ctx: Context):
        # ------------------------------------------------------------------------------
        # INIT
        if msg.type == MSGS.init:
            value = msg['value']
            if value not in self._rb_states:
                self._rb_states[value] = RB_STATES.echo
            rb_val = (value, self._id)
            broadcast(ctx, self._nodes, rb_create_msg(RB_MSGS.initial, rb_val))
        # ------------------------------------------------------------------------------

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # ------------------------------RELIABLE BROADCAST------------------------------
        # ------------------------------------------------------------------------------
        # INIT
        if msg.type == RB_MSGS.initial:
            rb_val = (msg['value'], msg['sender'])
            receivedCnt = self._rb_note_value(self._rb_initial_vals, rb_val, sender)
            if self._rb_states[rb_val] == RB_STATES.echo and receivedCnt >= 1:
                self._rb_send_echo(ctx, rb_val)
        # ------------------------------------------------------------------------------
        # ECHO
        elif msg.type == RB_MSGS.echo:
            rb_val = (msg['value'], msg['sender'])
            receivedCnt = self._rb_note_value(self._rb_echo_vals, rb_val, sender)
            if self._rb_states[rb_val] == RB_STATES.echo:
                if receivedCnt >= 2 * self._f_count + 1:
                    self._rb_send_echo(ctx, rb_val)
            elif self._rb_states[rb_val] == RB_STATES.ready:
                if receivedCnt >= 2 * self._f_count + 1:
                    self._rb_send_ready(ctx, rb_val)
        # ------------------------------------------------------------------------------
        # READY
        elif msg.type == RB_MSGS.ready:
            rb_val = (msg['value'], msg['sender'])
            receivedCnt = self._rb_note_value(self._rb_ready_vals, rb_val, sender)
            if self._rb_states[rb_val] == RB_STATES.echo:
                if receivedCnt >= self._f_count + 1:
                    self._rb_send_echo(ctx, rb_val)
            elif self._rb_states[rb_val] == RB_STATES.ready:
                if receivedCnt >= self._f_count + 1:
                    self._rb_send_ready(ctx, rb_val)
            elif self._rb_states[rb_val] == RB_STATES.accept:
                if receivedCnt >= 2 * self._f_count + 1:
                    self._rb_accept(rb_val)
        # ------------------------------------------------------------------------------

    def on_timer(self, timer_name: str, ctx: Context):
        pass
