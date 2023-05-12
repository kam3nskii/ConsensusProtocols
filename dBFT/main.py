from dslib import Context, Message, Node

from typing import List


class MSGS():
    init   = "INIT"
    est    = "EST"
    aux    = "AUX"
    result = "RESULT"


def broadcast(ctx, nodes, msg):
    for node in nodes:
        ctx.send(msg, node)


def decide(ctx, r, value, decided):
    if not decided:
        ctx.send_local(Message(MSGS.result,
                               {'value': value, 'round': r}))
    return True


def BV_Broadcast(ctx, nodes, r, value, broadcasted):
    if r not in broadcasted:
        broadcasted[r] = set()
    if value not in broadcasted[r]:
        broadcasted[r].add(value)
        broadcast(ctx, nodes, Message(MSGS.est,
                                      {'value': value, 'round': r}))


def BV_Delivery(ctx, nodes, r, value, bin_values):
    if r not in bin_values:
        bin_values[r] = set()
    if value not in bin_values[r]:
        bin_values[r].add(value)
        broadcast(ctx, nodes, Message(MSGS.aux,
                                      {'bin_values': list(bin_values[r]), 'round': r}))


def receiveEstMessage(storage, r, value, sender):
    if r not in storage:
        storage[r] = {}
    if value not in storage[r]:
        storage[r][value] = set()
    storage[r][value].add(sender)
    sendersCnt = len(storage[r][value])
    return sendersCnt


def receiveAuxMessage(storage, r, values, sender):
    if r not in storage:
        storage[r] = {0: set(), 1: set()}
    for value in values:
        storage[r][value].add(sender)


def checkAuxMessage(storage, r, values, bin_values, min_senders_cnt):
    if r not in bin_values:
        bin_values[r] = set()
    for value in values:
        sendersCnt = len(storage[r][value])
        if (sendersCnt < min_senders_cnt) or (value not in bin_values[r]):
            return False
    return True


class SafeBBC(Node):
    def __init__(self, node_id: str, nodes: List[str], faulty_count: int):
        self._id = node_id
        self._nodes = nodes
        self._f_count = faulty_count

        self._decided = False
        self._est = None
        self._round = 0
        self._bin_values = {}

        self._received_ests = {}
        self._broadcasted_ests = {}

        self._received_auxs = {}

    def on_local_message(self, msg: Message, ctx: Context):
        # ------------------------------------------------------------------------------
        # INIT
        if msg.type == MSGS.init:
            self._est = msg['value']
            self._round += 1
            BV_Broadcast(ctx, self._nodes, self._round,
                         self._est, self._broadcasted_ests)
        # ------------------------------------------------------------------------------

    def on_message(self, msg: Message, sender: str, ctx: Context):

        # ------------------------------------------------------------------------------
        # EST
        if msg.type == MSGS.est:
            r = msg['round']
            value = msg['value']

            sendersCnt = receiveEstMessage(
                self._received_ests, r, value, sender)

            if sendersCnt >= self._f_count + 1:
                BV_Broadcast(ctx, self._nodes, r, value,
                             self._broadcasted_ests)

            if sendersCnt >= 2 * self._f_count + 1:
                BV_Delivery(ctx, self._nodes, r, value, self._bin_values)
        # ------------------------------------------------------------------------------
        # AUX
        if msg.type == MSGS.aux:
            r = msg['round']
            values = set(msg['bin_values'])

            receiveAuxMessage(self._received_auxs, r, values, sender)

            if r != self._round:
                return

            if checkAuxMessage(self._received_auxs, self._round, values,
                               self._bin_values, len(self._nodes) - self._f_count):
                b = self._round % 2
                if len(values) == 1:
                    self._est = values.pop()
                    if self._est == b:
                        self._decided = decide(
                            ctx, self._round, self._est, self._decided)
                else:
                    self._est = b

                self._round += 1
                BV_Broadcast(ctx, self._nodes, self._round,
                             self._est, self._broadcasted_ests)
        # ------------------------------------------------------------------------------

    def on_timer(self, timer_name: str, ctx: Context):
        pass
