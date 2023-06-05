from dslib import Context, Message, Node

from typing import List


class MSGS():
    init        = "INIT"
    est         = "EST"
    coord_value = "COORD_VALUE"
    aux         = "AUX"
    result      = "RESULT"


class TMRS():
    coord = "COORD-TIMER"
    aux   = "AUX-TIMER"


def broadcast(ctx, nodes, msg):
    for node in nodes:
        ctx.send(msg, node)


def decide(ctx, r, value, decided):
    if decided is None:
        ctx.send_local(Message(MSGS.result,
                               {'value': value, 'round': r}))
        return r
    return decided


def BV_Broadcast(ctx, nodes, r, value, broadcasted):
    if r not in broadcasted:
        broadcasted[r] = set()
    if value not in broadcasted[r]:
        broadcasted[r].add(value)
        broadcast(ctx, nodes, Message(MSGS.est,
                                      {'value': value, 'round': r}))


def BV_ReceiveMessage(storage, r, value, sender):
    if r not in storage:
        storage[r] = {}
    if value not in storage[r]:
        storage[r][value] = set()
    storage[r][value].add(sender)
    sendersCnt = len(storage[r][value])
    return sendersCnt


def BV_Delivery(bin_values, r, value):
    if r not in bin_values:
        bin_values[r] = set()

    isNewValue = False
    isFirstDelivery = None
    if value not in bin_values[r]:
        isNewValue = True
        isFirstDelivery = (len(bin_values[r]) == 0)
        bin_values[r].add(value)
    return isNewValue, isFirstDelivery


def receive_aux_message(storage, r, values, sender):
    if r not in storage:
        storage[r] = {}
    storage[r][sender] = values
    return len(storage[r].keys())


def count_aux_values(storage, r):
    result = {0: set(), 1: set()}
    for sender, values in storage[r].items():
        for value in values:
            result[value].add(sender)
    return result


def validate_aux_message(values, r, bin_values, senders_by_value, min_senders_cnt):
    if r not in bin_values:
        bin_values[r] = set()
    for value in values:
        sendersCnt = len(senders_by_value[value])
        if (sendersCnt < min_senders_cnt) or (value not in bin_values[r]):
            return False
    return True


class SafeBBC(Node):
    def __init__(self, node_id: str, nodes: List[str], faulty_count: int, seed: int):
        self._id = node_id
        self._nodes = nodes
        self._f_count = faulty_count

        self._decided_round = None
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

            sendersCnt = BV_ReceiveMessage(
                self._received_ests, r, value, sender)

            if sendersCnt >= self._f_count + 1:
                BV_Broadcast(ctx, self._nodes, r, value,
                             self._broadcasted_ests)

            if sendersCnt >= 2 * self._f_count + 1:
                new, _ = BV_Delivery(self._bin_values, r, value)
                if new:
                    broadcast(ctx, self._nodes,
                              Message(MSGS.aux,
                                      {'bin_values': list(self._bin_values[r]), 'round': r}))
        # ------------------------------------------------------------------------------
        # AUX
        if msg.type == MSGS.aux:
            r = msg['round']
            values = set(msg['bin_values'])

            sendersCnt = receive_aux_message(
                self._received_auxs, r, values, sender)

            if r != self._round:
                return

            senders_by_value = count_aux_values(
                self._received_auxs, self._round)
            if validate_aux_message(values, self._round, self._bin_values,
                                    senders_by_value, len(self._nodes) - self._f_count):
                b = self._round % 2
                if len(values) == 1:
                    self._est = values.pop()
                    if self._est == b:
                        self._decided_round = decide(
                            ctx, self._round, self._est, self._decided_round)
                else:
                    self._est = b

                self._round += 1
                BV_Broadcast(ctx, self._nodes, self._round,
                             self._est, self._broadcasted_ests)
        # ------------------------------------------------------------------------------

    def on_timer(self, timer_name: str, ctx: Context):
        pass


class PsyncBBC(Node):
    def __init__(self, node_id: str, nodes: List[str], faulty_count: int, seed: int):
        self._id = node_id
        self._nodes = nodes
        self._f_count = faulty_count

        self._decided_round = None
        self._est = None
        self._aux = list()
        self._round = 0
        self._timeout = 1
        self._bin_values = {}

        self._coord = None
        self._received_coord_vals = {}

        self._received_ests = {}
        self._broadcasted_ests = {}

        self._received_auxs = {}

    def _receive_coord_value_message(self, r, sender, value):
        if r not in self._received_coord_vals:
            self._received_coord_vals[r] = {}
        self._received_coord_vals[r][sender] = value

    def _get_coord_value(self):
        if self._round not in self._received_coord_vals:
            return None
        if self._coord not in self._received_coord_vals[self._round]:
            return None
        return self._received_coord_vals[self._round][self._coord]


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

            sendersCnt = BV_ReceiveMessage(
                self._received_ests, r, value, sender)

            if sendersCnt >= self._f_count + 1:
                BV_Broadcast(ctx, self._nodes, r, value,
                             self._broadcasted_ests)

            if sendersCnt >= 2 * self._f_count + 1:
                new, first = BV_Delivery(self._bin_values, r, value)
                if new and first:
                    self._timeout += 1
                    ctx.set_timer(f"{TMRS.coord}-NODE-{self._id}", self._timeout)

                    self._coord = str((self._round - 1) % len(self._nodes))
                    if self._id == self._coord:
                        broadcast(ctx, self._nodes, Message(MSGS.coord_value,
                                                            {'value': value, 'round': r}))
        # ------------------------------------------------------------------------------
        # COORD_VALUE
        if msg.type == MSGS.coord_value:
            r = msg['round']
            value = msg['value']

            self._receive_coord_value_message(r, sender, value)
        # ------------------------------------------------------------------------------
        # AUX
        if msg.type == MSGS.aux:
            r = msg['round']
            values = set(msg['bin_values'])

            sendersCnt = receive_aux_message(
                self._received_auxs, r, values, sender)
            if sendersCnt == len(self._nodes) - self._f_count:
                ctx.set_timer(f"{TMRS.aux}-NODE-{self._id}", self._timeout)
        # ------------------------------------------------------------------------------

    def on_timer(self, timer_name: str, ctx: Context):
        # ------------------------------------------------------------------------------
        # COORD
        if timer_name.startswith(TMRS.coord):
            value_from_coord = self._get_coord_value()

            self._aux = list()
            if not (value_from_coord is None) and (value_from_coord in self._bin_values[self._round]):
                self._aux.append(value_from_coord)
            else:
                self._aux = list(self._bin_values[self._round])
            broadcast(ctx, self._nodes, Message(MSGS.aux,
                                                {'bin_values': self._aux, 'round': self._round}))
        # ------------------------------------------------------------------------------
        # AUX
        if timer_name.startswith(TMRS.aux):
            values = set()

            senders_by_value = count_aux_values(self._received_auxs, self._round)
            checked_msgs = []
            for _, values in self._received_auxs[self._round].items():
                if validate_aux_message(values, self._round, self._bin_values,
                                        senders_by_value, len(self._nodes) - self._f_count):
                    checked_msgs.append(list(values))
            if self._aux in checked_msgs:
                values = set(self._aux)
            elif len(checked_msgs) >= 1:
                values = set(checked_msgs[0])

            if len(values) == 0:
                ctx.set_timer(f"{TMRS.aux}-NODE-{self._id}", 1)
            else:
                b = self._round % 2
                if len(values) == 1:
                    self._est = values.pop()
                    if self._est == b:
                        self._decided_round = decide(
                            ctx, self._round, self._est, self._decided_round)
                else:
                    self._est = b

                if self._decided_round == self._round - 2:
                    return

                self._round += 1
                BV_Broadcast(ctx, self._nodes, self._round,
                             self._est, self._broadcasted_ests)
        # ------------------------------------------------------------------------------
