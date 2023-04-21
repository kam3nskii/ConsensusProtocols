from dslib import Context, Message, Node

from typing import List
import random


class MSGS():
    init = "INIT"
    res = "RESULT"
    vote = "VOTE"
    propose = "PROPOSE"


class BenOrNode(Node):
    def __init__(self, node_id: str, nodes: List[str], seed: int, quorum: int):
        self._id = node_id
        self._nodes = nodes
        self._quorum = quorum
        self._pref = None
        self._round = None
        self._input_votes = {}
        self._input_props = {}
        random.seed(seed + int(self._id))


    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == MSGS.init:
            self._pref = msg['val']
            self._round = 0
            msg = Message(MSGS.vote, {'pref': self._pref,
                                      'round': self._round})
            for node in self._nodes:
                ctx.send(msg, node)


    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == MSGS.vote:
            if msg['round'] not in self._input_votes:
                self._input_votes[msg['round']] = []
            self._input_votes[msg['round']].append(msg['pref'])
            if len(self._input_votes[msg['round']]) == self._quorum:
                prefs_sum = sum(self._input_votes[msg['round']])
                val = "?"
                if prefs_sum == 0 or prefs_sum == self._quorum:
                    val = f'{int(prefs_sum > 0)}'
                msg = Message(MSGS.propose, {'val': val,
                                             'round': self._round})
                for node in self._nodes:
                    ctx.send(msg, node)

        elif msg.type == MSGS.propose:
            if msg['round'] not in self._input_props:
                self._input_props[msg['round']] = []
            self._input_props[msg['round']].append(msg['val'])
            if len(self._input_props[msg['round']]) == self._quorum:
                everyoneDoubts = True
                for i in range(self._quorum):
                    if (self._input_props[msg["round"]][i]
                            != self._input_props[msg["round"]][0]) or \
                            ((self._input_props[msg["round"]][0] != '?')):
                        everyoneDoubts = False
                        break
                if everyoneDoubts:
                    self._pref = random.choice([0, 1])
                    self._round += 1
                    msg = Message(MSGS.vote, {'pref': self._pref,
                                              'round': self._round})
                    for node in self._nodes:
                        ctx.send(msg, node)
                else:
                    newPref = None
                    noDoubts = True
                    for val in self._input_props[msg["round"]]:
                        if val == '?':
                            noDoubts = False
                            continue
                        if newPref is None:
                            newPref = val
                    if noDoubts:
                        self._pref = int(newPref)
                        self._round += 1
                        ctx.send_local(Message(MSGS.res, {'val': newPref}))
                    else:
                        self._pref = int(newPref)
                        self._round += 1
                        msg = Message(MSGS.vote, {'pref': self._pref,
                                                  'round': self._round})
                        for node in self._nodes:
                            ctx.send(msg, node)


    def on_timer(self, timer_name: str, ctx: Context):
        pass
