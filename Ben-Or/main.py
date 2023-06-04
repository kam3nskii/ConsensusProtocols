from dslib import Context, Message, Node

from typing import List
import random


class MSGS():
    init = "INIT"
    vote = "VOTE"
    propose = "PROPOSE"
    result = "RESULT"


class VALS():
    doubt = '?'


def broadcast(ctx, nodes, msg):
    for node in nodes:
        ctx.send(msg, node)


def createMsg(type, value, round):
    return Message(type, {'value': value,
                          'round': round})


def decide(ctx, value):
    ctx.send_local(Message(MSGS.result, {'value': value}))


def getProposingValue(f_count, votes):
    tmp = {0: 0, 1: 0}
    for _, val in votes:
        tmp[val] += 1
    value = VALS.doubt
    for val, cnt in tmp.items():
        if cnt > 3 * f_count:
            value = f'{val}'
            break
    return value


class BenOrNode(Node):
    def __init__(self, node_id: str, nodes: List[str], faulty_count: int, seed: int):
        self._id = node_id
        self._nodes = nodes
        self._f_count = faulty_count
        random.seed(seed + int(self._id))

        self._pref = None
        self._round = None
        self._received_votes = {}
        self._received_proposes = {}

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == MSGS.init:
            self._pref = msg['value']
            self._round = 0
            broadcast(ctx, self._nodes, createMsg(
                MSGS.vote, self._pref, self._round))

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == MSGS.vote:
            r = msg['round']

            if r not in self._received_votes:
                self._received_votes[r] = {}
            self._received_votes[r][sender] = msg['value']

            if len(self._received_votes[r].keys()) == len(self._nodes) - self._f_count + 1:
                value = getProposingValue(
                    self._f_count, self._received_votes[r].items())
                broadcast(ctx, self._nodes, createMsg(
                    MSGS.propose, value, self._round))

        elif msg.type == MSGS.propose:
            r = msg['round']

            if r not in self._received_proposes:
                self._received_proposes[r] = {}
            self._received_proposes[r][sender] = msg['value']
            if len(self._received_proposes[r].keys()) == len(self._nodes) - self._f_count + 1:
                tmp = {'?': 0, '0': 0, '1': 0}
                for _, val in self._received_proposes[r].items():
                    tmp[val] += 1

                have_doubts = True
                for val, cnt in tmp.items():
                    if cnt >= self._f_count + 1 and val != VALS.doubt:
                        have_doubts = False
                        self._pref = int(val)
                        if cnt > 3 * self._f_count:
                            decide(ctx, self._pref)
                            return
                        continue
                if have_doubts:
                    self._pref = random.choice([0, 1])
                self._round += 1
                broadcast(ctx, self._nodes, createMsg(
                    MSGS.vote, self._pref, self._round))

    def on_timer(self, timer_name: str, ctx: Context):
        pass
