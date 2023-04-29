from dslib import Context, Message, Node

from typing import List
import random


class MSGS():
    init = "INIT"
    est = "EST"
    aux = "AUX"
    result = "RESULT"


def broadcast(ctx, nodes, msg):
    for node in nodes:
        ctx.send(msg, node)


def createMsgEst(value, round):
    return Message(MSGS.est, {'value': value,
                              'round': round})


def createMsgAux(values, round):
    return Message(MSGS.aux, {'bin_values': list(values),
                              'round': round})


def decide(ctx, value, decided):
    if not decided:
        ctx.send_local(Message(MSGS.result, {'value': value}))
    return True


class dBFTNode(Node):
    def __init__(self, node_id: str, nodes: List[str], seed: int, faulty_count: int):
        self._id = node_id
        self._nodes = nodes
        self._f_count = faulty_count
        random.seed(seed + int(self._id))

        self._est = None
        self._round = 0
        self._bin_values = {}
        self._b = None
        self._received_ests = {}
        self._received_auxs = {}
        self._decided = False

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == MSGS.init:
            self._est = msg['value']
            self._round += 1
            broadcast(ctx, self._nodes, createMsgEst(self._est, self._round))

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == MSGS.est:
            r = msg['round']
            value = msg['value']

            if r not in self._received_ests:
                self._received_ests[r] = {}
            if value not in self._received_ests[r]:
                self._received_ests[r][value] = set()
            if sender in self._received_ests[r][value]:
                return

            self._received_ests[r][value].add(sender)

            cnt = len(self._received_ests[r][value])
            if cnt == self._f_count + 1:
                broadcast(ctx, self._nodes, createMsgEst(value, self._round))
            if cnt == 2 * self._f_count + 1:
                if r not in self._bin_values:
                    self._bin_values[r] = set()
                self._bin_values[r].add(value)
                broadcast(ctx, self._nodes, createMsgAux(
                    self._bin_values[r], self._round))

        if msg.type == MSGS.aux:
            r = msg['round']
            values = set(msg['bin_values'])

            if r not in self._received_auxs:
                self._received_auxs[r] = {0: set(), 1: set()}

            alreadyDelivered = True
            for value in values:
                if sender not in self._received_auxs[r][value]:
                    alreadyDelivered = False
                    self._received_auxs[r][value].add(sender)
            if alreadyDelivered:
                return

            if values.issubset(self._bin_values):
                for value in values:
                    if len(self._received_auxs[r][value]) < len(self._nodes) - self._f_count:
                        return

                self._b = r % 2
                if len(values) == 1:
                    value = values.pop()
                    if value == self._b:
                        self._est = value
                        self._decided = decide(ctx, self._est, self._decided)
                    else:
                        self._est = self._b
                self._round += 1
                broadcast(ctx, self._nodes, createMsgEst(self._est, self._round))

    def on_timer(self, timer_name: str, ctx: Context):
        pass
