from dslib import Context, Message, Node

from typing import List


class RB_MSGS():
    initial = "RB_INIT"
    echo = "RB_ECHO"
    ready = "RB_READY"
    accept = "RB_ACCEPT"


class MSGS():
    init        = "INIT"
    est         = "EST"
    coord_value = "COORD_VALUE"
    aux         = "AUX"
    result      = "RESULT"


class RB_STATES():
    echo = 1
    ready = 2
    accept = 3
    done = 4


class TMRS():
    coord = "COORD-TIMER"
    aux   = "AUX-TIMER"


def broadcast(ctx, nodes, msg):
    for node in nodes:
        ctx.send(msg, node)


def rb_create_msg(type, rb_val):
    value, sender = rb_val
    return Message(type, {'value': value, 'sender': sender})


def BV_Broadcast(ctx, nodes, k, r, value, broadcasted):
    if r not in broadcasted:
        broadcasted[r] = set()
    if value not in broadcasted[r]:
        broadcasted[r].add(value)
        broadcast(ctx, nodes, Message(MSGS.est,
                                      {'k': k, 'value': value, 'round': r}))


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


class DBFT(Node):
    def __init__(self, node_id: str, nodes: List[str], faulty_count: int, seed: int):
        self._id = node_id
        self._nodes = nodes
        self._f_count = faulty_count

        self._rb_states = {}
        self._rb_initial_vals = {}
        self._rb_echo_vals = {}
        self._rb_ready_vals = {}

        self._proposals = [None] * len(self._nodes)
        self._bin_decisions = [None] * len(self._nodes)
        self._already_decided_one = False
        self._decided_proposal = False

        self._decided_round = [None for _ in range(len(self._nodes))]
        self._est = [None for _ in range(len(self._nodes))]
        self._aux = [[] for _ in range(len(self._nodes))]
        self._round = [0 for _ in range(len(self._nodes))]
        self._timeout = [1 for _ in range(len(self._nodes))]
        self._bin_values = [{} for _ in range(len(self._nodes))]
        self._coord = [None for _ in range(len(self._nodes))]
        self._received_coord_vals = [{} for _ in range(len(self._nodes))]
        self._received_ests = [{} for _ in range(len(self._nodes))]
        self._broadcasted_ests = [{} for _ in range(len(self._nodes))]
        self._received_auxs = [{} for _ in range(len(self._nodes))]

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

    def _rb_accept(self, ctx, rb_val):
        self._rb_states[rb_val] = RB_STATES.done
        value, sender = rb_val
        sender = int(sender)
        self._proposals[int(sender)] = value
        # print(f'[{self._id}]: RB-delivered from {sender}: {self._proposals}')

        if not self._already_decided_one:
            # print(f'[{self._id}]: BIN_COINS[{sender}].bin_propose(-1)')
            self._est[sender] = -1
            self._round[sender] += 1
            if self._est[sender] == -1:
                self._est[sender] = 1
                new, first = BV_Delivery(self._bin_values[sender], self._round[sender], self._est[sender])
                if new and first:
                    self._timeout[sender] += 1
                    ctx.set_timer(f'{TMRS.coord}-NODE-{self._id}|k={sender}', self._timeout[sender])

                    self._coord[sender] = str((self._round[sender] - 1) % len(self._nodes))
                    if self._id == self._coord[sender]:
                        broadcast(ctx, self._nodes, Message(MSGS.coord_value,
                                                            {'k': sender,
                                                            'value': self._est[sender],
                                                            'round': self._round[sender]}))

    def _receive_coord_value_message(self, k, r, sender, value):
        if r not in self._received_coord_vals[k]:
            self._received_coord_vals[k][r] = {}
        self._received_coord_vals[k][r][sender] = value

    def _get_coord_value(self, k):
        if self._round[k] not in self._received_coord_vals[k]:
            return None
        if self._coord[k] not in self._received_coord_vals[k][self._round[k]]:
            return None
        return self._received_coord_vals[k][self._round[k]][self._coord[k]]

    def _receive_aux_message(self, k, r, values, sender):
        if r not in self._received_auxs[k]:
            self._received_auxs[k][r] = {}
        self._received_auxs[k][r][sender] = values
        return len(self._received_auxs[k][r].keys())

    def _count_aux_values(self, k, r):
        result = {0: set(), 1: set()}
        for sender, values in self._received_auxs[k][r].items():
            for value in values:
                result[value].add(sender)
        return result

    def _validate_aux_message(self, k, values, senders_by_value):
        r = self._round[k]
        if r not in self._bin_values[k]:
            self._bin_values[k][r] = set()
        for value in values:
            sendersCnt = len(senders_by_value[value])
            min_senders_cnt = len(self._nodes) - self._f_count
            if (sendersCnt < min_senders_cnt) or (value not in self._bin_values[k][r]):
                return False
        return True

    def _bin_decide(self, ctx, k):
        if self._decided_round[k] is None:
            self._decided_round[k] = self._round[k]
            self._bin_decisions[k] = self._est[k]
            # print(f'[{self._id}]: BIN_COINS[{k}] decided {self._est[k]} in round {self._round[k]}')

            if not self._already_decided_one and self._est[k] == 1:
                self._already_decided_one = True
                for i in range(len(self._nodes)):
                    bin_coin_invoked = self._round[i]
                    if not bin_coin_invoked:
                        # print(f'[{self._id}]: BIN_COINS[{i}].bin_propose(0)')
                        self._est[i] = 0
                        self._round[i] += 1
                        BV_Broadcast(ctx, self._nodes, i, self._round[i],
                                    self._est[i], self._broadcasted_ests[i])

            if not (None in self._bin_decisions):
                j = None
                for i in range(len(self._bin_decisions)):
                    if self._bin_decisions[i] == 1:
                        j = i
                        break
                if not (self._proposals[j] is None):
                    self._decide_proposal(ctx, self._proposals[j])
                else:
                    # TODO
                    pass

    def _decide_proposal(self, ctx, value):
        if not self._decided_proposal:
            self._decided_proposal = True
            ctx.send_local(Message(MSGS.result,
                                   {'value': value}))

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
                    self._rb_accept(ctx, rb_val)
        # ------------------------------------------------------------------------------

        # ----------------------------------BIN COINS-----------------------------------
        # ------------------------------------------------------------------------------
        # COORD_VALUE
        if msg.type == MSGS.coord_value:
            k = msg['k']
            r = msg['round']
            value = msg['value']

            self._receive_coord_value_message(k, r, sender, value)
        # ------------------------------------------------------------------------------
        # AUX
        if msg.type == MSGS.aux:
            k = msg['k']
            r = msg['round']
            values = set(msg['bin_values'])

            sendersCnt = self._receive_aux_message(k, r, values, sender)
            if sendersCnt == len(self._nodes) - self._f_count:
                ctx.set_timer(f'{TMRS.aux}-NODE-{self._id}|k={k}', self._timeout[k])
        # ------------------------------------------------------------------------------
        # EST
        if msg.type == MSGS.est:
            k = msg['k']
            r = msg['round']
            value = msg['value']

            sendersCnt = BV_ReceiveMessage(self._received_ests[k], r, value, sender)

            if sendersCnt >= self._f_count + 1:
                BV_Broadcast(ctx, self._nodes, k, r, value, self._broadcasted_ests[k])

            if sendersCnt >= 2 * self._f_count + 1:
                new, first = BV_Delivery(self._bin_values[k], r, value)
                if new and first:
                    self._timeout[k] += 1
                    ctx.set_timer(f"{TMRS.coord}-NODE-{self._id}|k={k}", self._timeout[k])

                    self._coord[k] = str((self._round[k] - 1) % len(self._nodes))
                    if self._id == self._coord[k]:
                        broadcast(ctx, self._nodes, Message(MSGS.coord_value,
                                                            {'k': k, 'value': value, 'round': r}))
        # ------------------------------------------------------------------------------

    def on_timer(self, timer_name: str, ctx: Context):
        # ------------------------------------------------------------------------------
        # COORD
        if timer_name.startswith(TMRS.coord):
            k = int(timer_name[timer_name.find('k=') + 2:])
            value_from_coord = self._get_coord_value(k)

            self._aux[k] = list()
            if not (value_from_coord is None) and (value_from_coord in self._bin_values[k][self._round[k]]):
                self._aux[k].append(value_from_coord)
            else:
                self._aux[k] = list(self._bin_values[k][self._round[k]])
            broadcast(ctx, self._nodes, Message(MSGS.aux,
                                                {'k': k, 'bin_values': self._aux[k], 'round': self._round[k]}))
        # ------------------------------------------------------------------------------
        # AUX
        if timer_name.startswith(TMRS.aux):
            k = int(timer_name[timer_name.find('k=') + 2:])
            values = set()

            senders_by_value = self._count_aux_values(k, self._round[k])
            checked_msgs = []
            for _, values in self._received_auxs[k][self._round[k]].items():
                if self._validate_aux_message(k, values, senders_by_value):
                    checked_msgs.append(list(values))

            if self._aux[k] in checked_msgs:
                values = set(self._aux[k])
            elif len(checked_msgs) >= 1:
                values = set(checked_msgs[0])

            if len(values) == 0:
                ctx.set_timer(f"{TMRS.aux}-NODE-{self._id}|k={k}", 1)
            else:
                b = self._round[k] % 2
                if len(values) == 1:
                    self._est[k] = values.pop()
                    if self._est[k] == b:
                        self._bin_decide(ctx, k)
                else:
                    self._est[k] = b

                if self._decided_round[k] == self._round[k]:
                    pass
                    # TODO wait until bin_values = {0, 1}
                elif self._decided_round[k] == self._round[k] - 2:
                    return

                self._round[k] += 1
                BV_Broadcast(ctx, self._nodes, k, self._round[k], self._est[k], self._broadcasted_ests[k])
        # ------------------------------------------------------------------------------
