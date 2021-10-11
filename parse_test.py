#!/usr/bin/env python3

import abc
import argparse
import datetime
import math
import parse
import re
import typing


class Time():
    mainfmt = '%Y-%m-%d %H:%M:%S'

    def __init__(self, dt: datetime.datetime, ns: int):
        self.dt = dt
        self.ns = ns
        return

    def __str__(self) -> str:
        ns_str = str(self.ns)
        return self.dt.strftime(Time.mainfmt) + '.' + '0'*(9-len(ns_str)) + ns_str

    def __repr__(self) -> str:
        return f'parse_test.Time({self.dt}, {self.ns})'

    def __lt__(self, other) -> bool:
        if self.dt < other.dt:
            return True
        if self.dt > other.dt:
            return False
        return self.ns < other.ns

    def __le__(self, other) -> bool:
        if self.dt < other.dt:
            return True
        if self.dt > other.dt:
            return False
        return self.ns <= other.ns

    def __eq__(self, other) -> bool:
        return self.dt == other.dt and self.ns == other.ns

    def __ne__(self, other) -> bool:
        return self.dt != other.dt or self.ns != other.ns

    def __gt__(self, other) -> bool:
        if self.dt > other.dt:
            return True
        if self.dt < other.dt:
            return False
        return self.ns > other.ns

    def __ge__(self, other) -> bool:
        if self.dt > other.dt:
            return True
        if self.dt < other.dt:
            return False
        return self.ns >= other.ns

    def __sub__(self, other) -> float:
        return (self.dt - other.dt).total_seconds() + (self.ns - other.ns)/1e9

    pass


def time_add_secs(base: Time, secs: float):
    newns = round(base.ns + secs*1e9)
    ds = math.floor(newns / 1e9)
    rem_ns = int(newns - ds*1e9)
    return Time(base.dt + datetime.timedelta(seconds=ds), rem_ns)


def time_parse(formatted: str) -> Time:
    parts = formatted.split('.')
    dt = datetime.datetime.strptime(parts[0], Time.mainfmt)
    ns_str = parts[1] + '0' * (9 - len(parts[1]))
    return Time(dt, int(ns_str))


def time_blend(t0: Time, t1: Time, alpha: float) -> Time:
    ds = t1 - t0
    return time_add_secs(t0, ds * alpha)


class Queue:
    pass


class ProgressNoter():

    def __init__(self) -> None:
        super(ProgressNoter, self).__init__()
        self.trs: typing.List[typing.Tuple[Time, float]] = []
        return

    def add_progress_point(self, real_time: Time, R: float) -> None:
        nu = (real_time, R)
        if self.trs:
            if self.trs[-1] == nu:
                return
            if self.trs[-1][0] > real_time:
                raise Exception(
                    f'Time went backward: {self.trs[-1][0]} then {real_time}')
            if self.trs[-1][1] > R:
                raise Exception(f'R went backward: {self.trs[-1][1]} then {R}')
        self.trs.append(nu)
        return

    def applytr(self, x, ix: int, iy: int, blendy, extrapolate) -> float:
        if len(self.trs) == 0:
            raise Exception('empty relation')
        xmin, xmax = self.trs[0][ix], self.trs[-1][ix]
        if x < xmin:
            raise Exception(f'{x} out of range [{xmin},{xmax}]')
        if x > xmax:
            return extrapolate(self.trs[-1][iy], x-xmax)
        for i in range(len(self.trs)-1):
            if x == self.trs[i][ix]:
                return self.trs[i][iy]
            if x < self.trs[i+1][ix]:
                xpart = x - self.trs[i][ix]
                xfull = self.trs[i+1][ix] - self.trs[i][ix]
                return blendy(self.trs[i][iy], self.trs[i+1][iy], xpart/xfull)
        return self.trs[-1][iy]

    def R_of_t(self, t: Time) -> float:
        return self.applytr(t, 0, 1, lambda y0, y1, alpha: y0 + alpha*(y1-y0),
                            lambda R, dt: R + dt)

    def t_of_R(self, R: float) -> Time:
        return self.applytr(R, 1, 0, time_blend, lambda t, dr: time_add_secs(t, dr))


def duration_parse(as_str: str) -> float:
    slen = len(as_str)
    if as_str.endswith('ms'):
        return float(as_str[:-2])/1.0e3
    if as_str.endswith('us'):
        return float(as_str[:-2])/1.0e6
    if as_str.endswith('\xb5s'):
        return float(as_str[:-2])/1.0e6
    if as_str.endswith('ns'):
        return float(as_str[:-2])/1.0e9
    if as_str.endswith('s'):
        return float(as_str[:-1])
    raise Exception(f'Unexpected duration string {as_str}')

def runs_prefix(runs: typing.List[typing.List[int]], take: int) -> typing.List[typing.List[int]]:
    ans = []
    while take > 0 and len(runs)> 0:
        delta = min(take, runs[0][1])
        ans.append([runs[0][0], delta])
        take -= delta
        runs = runs[1:]
    return ans

class Request():
    def __init__(self, id: typing.Tuple[int, int, int]):
        self.id = id
        self.qlane = int(-1)
        return

    def set_dispatch(self, real_dispatch_t_str: str, real_dispatch_r_str: str, queue_str: str, width1_str: str, width2_str: str, pad_str: str, virt_dispatch_r_str: str, seat_finder: typing.Callable[[int], typing.List[typing.List[int]]]):
        self.real_dispatch_t = time_parse(real_dispatch_t_str)
        self.real_dispatch_r = float(real_dispatch_r_str)
        self.queue = int(queue_str)
        self.width1 = int(width1_str)
        self.width2 = int(width2_str) if width2_str else 0
        self.width = max(self.width1, self.width2)
        self.duration2 = duration_parse(pad_str)
        # This is the R of the currently scheduled dispatch in the virtual world,
        # which may be revised later after the actual duration of earlier requests
        # is learned.
        self.virt_dispatch_r = float(virt_dispatch_r_str)
        self.seat_runs = seat_finder(self.width)
        self.seat_runs1 = runs_prefix(self.seat_runs, self.width1)
        # print(f'Request {self.id} assigned seat runs {self.seat_runs}')
        return

    def set_finish(self, real_finish_t_str: str, real_finish_r_str: str, queue_str: str, width_str: str, duration_str: str, seat_releaser: typing.Callable[[typing.List[typing.List[int]]], None]):
        self.real_finish_t = time_parse(real_finish_t_str)
        self.real_finish_r = float(real_finish_r_str)
        self.real_mid_t, self.real_mid_r = self.real_finish_t, self.real_finish_r
        queue = int(queue_str)
        if self.queue != queue:
            raise Exception(
                f'Queue mismatch for {self.id}: {self.queue} then {queue}')
        width = int(width_str)
        if self.width != width:
            raise Exception(
                f'Width mismatch for {self.id}: {self.width} then {width}')
        self.duration1 = float(duration_str)
        self.duration2 = 0.0
        self.duration = self.duration1 + self.duration2
        # print(f'Request {self.id} releasing seat runs {self.seat_runs}')
        seat_releaser(self.seat_runs)
        return

    def set_mid(self, real_mid_t_str: str, real_mid_r_str: str, queue_str: str, width1_str: str, pad_str: str, duration_str: str):
        self.real_mid_t = time_parse(real_mid_t_str)
        self.real_mid_r = float(real_mid_r_str)
        queue = int(queue_str)
        if self.queue != queue:
            raise Exception(
                f'Queue mismatch for {self.id}: {self.queue} then {queue}')
        width1 = int(width1_str)
        if self.width1 != width1:
            raise Exception(
                f'Width2 mismatch for {self.id}: {self.width1} then {width1}')
        pad = float(pad_str)
        if pad != self.duration2:
            raise Exception(
                f'Pad mismatch for {self.id}: {self.pad} the {pad}')
        self.duration1 = float(duration_str)
        return

    def finish_linger(self, real_finish_t_str: str, real_finish_r_str: str, queue_str: str, seat_releaser: typing.Callable[[typing.List[typing.List[int]]], None]):
        self.real_finish_t = time_parse(real_finish_t_str)
        self.real_finish_r = float(real_finish_r_str)
        queue = int(queue_str)
        if self.queue != queue:
            raise Exception(
                f'Queue mismatch for {self.id}: {self.queue} then {queue}')
        self.duration = self.duration1 + self.duration2
        seat_releaser(self.seat_runs)
        return

    def complete(self, t_of_R: typing.Callable[[float], Time]) -> None:
        try:
            self.virt_dispatch_t = t_of_R(self.virt_dispatch_r)
            work1 = self.width1 * self.duration1
            work2 = self.width2 * self.duration2
            # And thus these times are based on the virtual world
            # dispatch time that was expected when the real world dispatch happened.
            self.virt_mid_r = self.virt_dispatch_r + work1
            self.virt_mid_t = t_of_R(self.virt_mid_r)
            self.virt_finish_r = self.virt_mid_r + work2
            self.virt_finish_t = t_of_R(self.virt_finish_r)
        except Exception as exn:
            print(f'Exception completing {self.as_dict()}')
            raise exn

    def as_dict(self) -> dict:
        ans = dict(id=self.id,
                   queue=self.queue,
                   width1=self.width1,
                   width2=self.width2,
                   duration2=self.duration2,
                   real_dispatch_t=str(self.real_dispatch_t),
                   real_dispatch_r=self.real_dispatch_r,
                   virt_dispatch_r=self.virt_dispatch_r,
                   seat_runs=self.seat_runs,
                   seat_runs1=self.seat_runs1,
                   )
        try:
            ans['real_mid_t'] = str(self.real_mid_t)
        except AttributeError:
            pass
        try:
            ans['real_finish_t'] = str(self.real_finish_t)
            ans['real_finish_r'] = self.real_finish_r
            ans['duration1'] = self.duration1
        except AttributeError:
            pass
        try:
            ans['virt_dispatch_t'] = str(self.virt_dispatch_t)
            ans['virt_finish_r'] = self.virt_finish_r
            ans['virt_finish_t'] = str(self.virt_finish_t)
        except AttributeError:
            pass
        return ans


class SeatAllocator():

    def __init__(self):
        super(SeatAllocator, self).__init__()
        self.seats: typing.List[bool] = []
        return

    def find_seats(self, width: int) -> typing.List[typing.List[int]]:
        runs = []
        for _ in range(width):
            # find a seat
            seat = None
            for i in range(len(self.seats)):
                if not self.seats[i]:
                    seat = i
                    self.seats[i] = True
                    break
            if seat is None:
                seat = len(self.seats)
                self.seats.append(True)
            # appends to runs
            if runs and runs[-1][0] + runs[-1][1] == seat:
                runs[-1][1] += 1
            else:
                runs.append([seat, 1])
        return runs

    def release_seats(self, runs: typing.List[typing.List[int]]) -> None:
        for run in runs:
            for seat in range(run[0], run[0]+run[1]):
                self.seats[seat] = False
        return

    pass


class TestParser(parse.Parser, SeatAllocator, ProgressNoter):

    def __init__(self):
        super(TestParser, self).__init__()
        self.requests: typing.Mapping[typing.Tuple[int,
                                                   int, int], Request] = dict()
        self.cases: typing.List[typing.Tuple[re.Pattern,
                                             typing.Callable[[re.Match], None]]] = []
        self.num_queues: int = 0
        self.queue_to_lanes: typing.Mapping[int, SeatAllocator] = dict()
        self.queue_lane_sum: int = 0
        self.max_flow: int = 0
        self.min_t = Time(datetime.datetime(2050, 1, 1), 0)
        self.max_t = Time(datetime.datetime(2000, 1, 1), 0)

        def consume_dispatch(match: re.Match) -> None:
            # print(f'Parsed {match.groupdict()}')
            req = self.get_req(match.group('flow'), match.group(
                'thread'), match.group('iter'))
            req.set_dispatch(match.group('realStartT'), match.group('realStartR'), match.group(
                'queue'), match.group('width1'), match.group('width2'), match.group('pad'), match.group('virtStartR'), self.find_seats)
            self.add_progress_point(req.real_dispatch_t, req.real_dispatch_r)

        self.add_case(r'I[0-9]{4} [0-9.:]+\s+[0-9]+ queueset\.go:[0-9]+\] QS\(.*\) at [tr]=(?P<realStartT>[-0-9 .:]+) [vR]=(?P<realStartR>[0-9.]+)ss: dispatching request "(?P<desc1>.*)" \[\]int\{(?P<flow>[0-9]+), (?P<thread>[0-9]+), (?P<iter>[0-9]+)\} work \{\{?(?P<width1>[0-9]+)( (?P<width2>[0-9]+))? (?P<pad>[0-9.]+[mun' '\xb5' r']?s)(\} [0-9]+)\} from queue (?P<queue>[0-9]+) with start R (?P<virtStartR>[0-9.]+)ss, queue will have [0-9]+ waiting & [0-9]+ requests occupying [0-9]+ seats, set will have [0-9]+ seats occupied',
                      consume_dispatch)

        def consume_finish(match: re.Match) -> None:
            req = self.get_req(match.group('flow'), match.group(
                'thread'), match.group('iter'))
            req.set_finish(match.group('realEndT'), match.group('realEndR'), match.group(
                'queue'), match.group('width'), match.group('duration'), self.release_seats)
            self.add_progress_point(req.real_finish_t, req.real_finish_r)

        self.add_case(r'I[0-9]{4} [0-9.:]+\s+[0-9]+ queueset\.go:[0-9]+\] QS(.*) at [rt]=(?P<realEndT>[-0-9 .:]+) [vR]=(?P<realEndR>[0-9.]+)ss: request "(?P<desc1>.*)" \[\]int\{(?P<flow>[0-9]+), (?P<thread>[0-9]+), (?P<iter>[0-9]+)\} finished all use of (?P<width>[0-9]+) seats, adjusted queue (?P<queue>[0-9]+) start R to (?P<newStartR>[0-9.]+)ss due to service time (?P<duration>[0-9.]+)s, queue will have \d+ requests, \d+ seats waiting & \d+ requests occupying \d+ seats',
                      consume_finish)
        self.add_case(r'I[0-9]{4} [0-9.:]+\s+[0-9]+ queueset\.go:[0-9]+\] QS(.*) at [rt]=(?P<realEndT>[-0-9 .:]+) [vR]=(?P<realEndR>[0-9.]+)ss: request "(?P<desc1>.*)" \[\]int\{(?P<flow>[0-9]+), (?P<thread>[0-9]+), (?P<iter>[0-9]+)\} finished all use of (?P<width>[0-9]+) seats, adjusted queue (?P<queue>[0-9]+) start R to (?P<newStartR>[0-9.]+)ss due to service time (?P<duration>[0-9.]+)s, queue sum: queueset.queueSum\{InitialSeatsSum:\d+, MaxSeatsSum:\d+, TotalWorkSum:[0-9a-fA-Fx]+\}, \d+ requests waiting & \d+ requests occupying \d+ seats',
                      consume_finish)

        def consume_mid(match: re.Match) -> None:
            req = self.get_req(match.group('flow'), match.group(
                'thread'), match.group('iter'))
            req.set_mid(match.group('realMidT'), match.group('realMidR'), match.group(
                'queue'), match.group('width1'), match.group('pad'), match.group('duration'))
            self.add_progress_point(req.real_mid_t, req.real_mid_r)

        self.add_case(r'I[0-9]{4} [0-9.:]+\s+[0-9]+ queueset\.go:[0-9]+\] QS(.*) at [rt]=(?P<realMidT>[-0-9 .:]+) [vR]=(?P<realMidR>[0-9.]+)ss: request "(?P<desc1>.*)" \[\]int\{(?P<flow>[0-9]+), (?P<thread>[0-9]+), (?P<iter>[0-9]+)\} finished main use but lingering on (?P<width1>[0-9]+) seats for (?P<pad>[0-9.]+) seconds, adjusted queue (?P<queue>[0-9]+) start R to (?P<newStartR>[0-9.]+)ss due to service time (?P<duration>[0-9.]+)s, queue will have \d+ requests with queueset.queueSum\{.*\} waiting & \d+ requests occupying \d+ seats',
                      consume_mid)

        def consume_linger_finish(match: re.Match) -> None:
            req = self.get_req(match.group('flow'), match.group(
                'thread'), match.group('iter'))
            req.finish_linger(match.group('realEndT'), match.group('realEndR'), match.group(
                'queue'), self.release_seats)
            self.add_progress_point(req.real_finish_t, req.real_finish_r)

        self.add_case(r'I[0-9]{4} [0-9.:]+\s+[0-9]+ queueset\.go:[0-9]+\] QS(.*) at [rt]=(?P<realEndT>[-0-9 .:]+) [vR]=(?P<realEndR>[0-9.]+)ss: request "(?P<desc1>.*)" \[\]int\{(?P<flow>[0-9]+), (?P<thread>[0-9]+), (?P<iter>[0-9]+)\} finished lingering on (?P<width1>[0-9]+) seats, queue (?P<queue>[0-9]+) will have \d+ requests with queueset.queueSum\{.*\} waiting & \d+ requests occupying \d+ seats',
                      consume_linger_finish)

        def consume_end(match: re.Match) -> None:
            self.eval_t = time_parse(match.group('evalTime'))
            return
        self.add_case(
            r'\s*queueset_test\.go:\d+: (?P<evalTime>[-0-9 .:]+): End', consume_end)
        return

    def get_req(self, flow_str: str, thread_str: str, iter_str: str) -> Request:
        reqid = (int(flow_str), int(thread_str), int(iter_str))
        req = self.requests.get(reqid)
        if not req:
            req = Request(reqid)
            self.requests[reqid] = req
        return req

    def parse(self, file) -> None:
        super().parse(file)
        queue_to_active: typing.Mapping[int, typing.List[Request]] = dict()
        for (reqid, req) in self.requests.items():
            req.complete(self.t_of_R)
            self.max_flow = max(self.max_flow, reqid[0])
            self.num_queues = max(self.num_queues, req.queue)
            lanes = self.queue_to_lanes.get(req.queue)
            if lanes is None:
                lanes = SeatAllocator()
                self.queue_to_lanes[req.queue] = lanes
            active = queue_to_active.get(req.queue)
            if active is None:
                queue_to_active[req.queue] = [req]
            else:
                newActive: typing.List[Request] = []
                for oreq in active:
                    if req.virt_dispatch_r >= oreq.virt_finish_r:
                        lanes.release_seats([[oreq.qlane, 1]])
                    else:
                        newActive.append(oreq)
                newActive.append(req)
                queue_to_active[req.queue] = newActive
            req.qlane = lanes.find_seats(1)[0][0]
            self.min_t = min(self.min_t, min(
                req.real_dispatch_t, req.virt_dispatch_t))
            self.max_t = max(self.max_t,
                             req.real_finish_t,
                             req.virt_finish_t,
                             )
        for (qid, lanes) in self.queue_to_lanes.items():
            qlanes = len(lanes.seats)
            self.queue_lane_sum += qlanes
        return

    pass


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description='parse queueset test log')
    arg_parser.add_argument('infile', type=argparse.FileType('rt'))
    args = arg_parser.parse_args()
    test_parser = TestParser()
    test_parser.parse(args.infile)
    for (reqid, req) in test_parser.requests.items():
        print(req.as_dict())
    pass
