#!/usr/bin/env python3

import argparse
import datetime
import math
import parse
import re
import typing

class Time():
    mainfmt = '%Y-%m-%d %H:%M:%S'

    def __init__(self, dt:datetime.datetime, ns:int):
        self.dt = dt
        self.ns = ns
        return
    
    def __str__(self) -> str:
        ns_str = str(self.ns)
        return self.dt.strftime(Time.mainfmt) + '.' + '0'*(9-len(ns_str)) + ns_str

    def __repr__(self) -> str:
        return f'parse_test.Time({self.dt}, {self.ns})'
    
    def __lt__(self, other) -> bool:
        if self.dt < other.dt: return True
        if self.dt > other.dt: return False
        return self.ns < other.ns
    
    def __le__(self, other) -> bool:
        if self.dt < other.dt: return True
        if self.dt > other.dt: return False
        return self.ns <= other.ns
    
    def __eq__(self, other) -> bool:
        return self.dt == other.dt and self.ns == other.ns
    
    def __ne__(self, other) -> bool:
        return self.dt != other.dt or self.ns != other.ns
    
    def __gt__(self, other) -> bool:
        if self.dt > other.dt: return True
        if self.dt < other.dt: return False
        return self.ns > other.ns
    
    def __ge__(self, other) -> bool:
        if self.dt > other.dt: return True
        if self.dt < other.dt: return False
        return self.ns >= other.ns
    
    def __sub__(self, other) -> float:
        return (self.dt - other.dt).total_seconds() + (self.ns - other.ns)/1e9

    pass

def time_add_secs(base:Time, secs:float):
        newns = base.ns + secs*1e9
        ds = math.floor(newns / 1e9)
        rem_ns = newns - ds*1e9
        return Time(base.dt + datetime.timedelta(seconds=ds), rem_ns)


def time_parse(formatted:str) -> Time:
        parts = formatted.split('.')
        dt = datetime.datetime.strptime(parts[0], Time.mainfmt)
        ns_str = parts[1] + '0' * (9 - len(parts[1]))
        return Time(dt, int(ns_str))
        

def time_blend(t0:Time, t1:Time, alpha:float) -> Time:
        ds = t1 - t0
        return time_add_secs(t0, ds * alpha)

class Queue:
    pass

class Request:
    def __init__(self, id:typing.Tuple[int, int, int]):
        self.id = id
        return
    
    def set_dispatch(self, real_dispatch_str:str, real_dispatch_r_str:str, queue_str:str, width_str:str, virt_dispatch_r_str:str):
        self.real_dispatch = time_parse(real_dispatch_str)
        self.real_dispatch_r = float(real_dispatch_r_str)
        self.parser.add_rv(self.real_dispatch, self.real_dispatch_r)
        self.queue = int(queue_str)
        self.width = int(width_str)
        self.virt_dispatch_r = float(virt_dispatch_r_str)
        return
    
    def set_finish(self, real_finish_str:str, real_finish_r_str:str, queue_str:str, width_str:str, duration_str:str):
        self.real_finish = time_parse(real_finish_str)
        self.real_finish_r = float(real_finish_r_str)
        self.parser.add_rv(self.real_finish, self.real_finish_r)
        queue = int(queue_str)
        if self.queue != queue:
            raise Exception(f'Queue mismatch for {self.id}: {self.queue} then {queue}')
        width = int(width_str)
        if self.width != width:
            raise Exception('Width mismatch for {self.id}: {self.width} then {width}')
        self.duration = float(duration_str)
        return

    def as_dict(self) -> dict:
        return dict(id=self.id,
                    queue=self.queue,
                    width=self.width,
                    duration=self.duration,
                    real_dispatch=self.real_dispatch,
                    real_finish=self.real_finish,
                    virt_dispatch=self.virt_dispatch,
                    virt_finish=self.virt_finish,
                    real_dispatch_r=self.real_dispatch_r,
                    real_finish_r=self.real_finish_r,
                    virt_dispatch_r=self.virt_dispatch_r,
                    virt_finish_r=self.virt_finish_r,
                    )

class Seat:
    pass

class TestParser(parse.Parser):
    @classmethod
    def init_queues(C) -> typing.Mapping[int, Queue]:
        return {}

    @classmethod
    def init_requests(C) -> typing.Mapping[typing.Tuple[int, int, int], Request]:
        return {}

    @classmethod
    def init_rvs(C) -> typing.List[typing.Tuple[float, float]]:
        return []

    @classmethod
    def init_cases(c) -> typing.List[typing.Tuple[re.Pattern, typing.Callable[[re.Match], None]]]:
        return []
    
    def applyrv(self, x, ix:int, iy:int, blendy) -> float:
        if len(self.rvs) == 0:
            raise Exception('empty relation')
        xmin, xmax = self.rvs[0][ix], self.rvs[-1][ix]
        if x < xmin or x > xmax:
            raise Exception(f'{t} out of range [{xmin},{xmax}]')
        for i in range(len(self.rvs)-1):
            if x == self.rvs[i][ix]:
                return self.rvs[i][iy]
            if x < self.rvs[i+1][ix]:
                xpart = x - self.rvs[i][ix]
                xfull = self.rvs[i+1][ix] - self.rvs[i][ix]
                return blendy(self.rvs[i][iy], self.rvs[i+1][iy], xpart/xfull)
        return self.rvs[-1][1]

    def R_of_t(self, t:Time) -> float:
        return self.applyrv(t, 0, 1, lambda y0, y1, alpha: y0 + alpha*(y1-y0))

    def t_of_R(self, R:float) -> Time:
        return self.applyrv(R, 1, 0, time_blend)
    
    def __init__(self):
        self.queues = TestParser.init_queues()
        self.requests = TestParser.init_requests()
        self.rvs = TestParser.init_rvs()
        self.cases = TestParser.init_cases()
        self.add_case(r'I[0-9]{4} [0-9.:]+\s+[0-9]+ queueset\.go:[0-9]+\] QS\(.*\) at r=(?P<realStart>[-0-9 .:]+) v=(?P<realStartR>[0-9.]+)ss: dispatching request "(?P<desc1>.*)" \[\]int\{(?P<flow>[0-9]+), (?P<thread>[0-9]+), (?P<iter>[0-9]+)\} work \{(?P<width>[0-9]+) (?P<pad>[0-9.]+)s\} from queue (?P<queue>[0-9]+) with start R (?P<dispatchR>[0-9.]+)ss, queue will have [0-9]+ waiting & [0-9]+ requests occupying [0-9]+ seats, set will have [0-9]+ seats occupied',
                      lambda match: self.get_req(match.group('flow'), match.group('thread'), match.group('iter')).set_dispatch(match.group('realStart'), match.group('realStartR'), match.group('queue'), match.group('width'), match.group('dispatchR')))
        self.add_case(r'I[0-9]{4} [0-9.:]+\s+[0-9]+ queueset\.go:[0-9]+\] QS(.*) at r=(?P<realEnd>[-0-9 .:]+) v=(?P<realEndR>[0-9.]+)ss: request "(?P<desc1>.*)" \[\]int\{(?P<flow>[0-9]+), (?P<thread>[0-9]+), (?P<iter>[0-9]+)\} finished all use of (?P<width>[0-9]+) seats, adjusted queue (?P<queue>[0-9]+) start R to (?P<newStartR>[0-9.]+)ss due to service time (?P<duration>[0-9.]+)s, queue will have \d requests, \d seats waiting & \d requests occupying \d seats',
                      lambda match: self.get_req(match.group('flow'), match.group('thread'), match.group('iter')).set_finish(match.group('realEnd'), match.group('realEndR'), match.group('queue'), match.group('width'), match.group('duration')))
        return

    def add_rv(self, real_time:float, R:float) -> None:
        nu = (real_time, R)
        if self.rvs:
            if self.rvs[-1] == nu:
                return
            if self.rvs[-1][0] > real_time:
                raise Exception(f'Time went backward: {self.rvs[-1][0]} then {real_time}')
            if self.rvs[-1][1] > R:
                raise Exception(f'R went backward: {self.rvs[-1][1]} then {R}')
        self.rvs.append(nu)
        return

    def get_req(self, flow_str:str, thread_str:str, iter_str:str) -> Request:
        reqid = (int(flow_str), int(thread_str), int(iter_str))
        req = self.requests.get(reqid)
        if not req:
            req = Request(reqid)
            req.parser = self
            self.requests[reqid] = req
        return req

    def parse(self, file) -> None:
        super().parse(file)
        for (reqid,req) in self.requests.items():
            req.virt_dispatch = self.t_of_R(req.virt_dispatch_r)
            req.virt_finish_r = req.virt_dispatch_r + req.duration * req.width
            req.virt_finish = self.t_of_R(req.virt_finish_r)
        return
    
    pass

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description='parse queueset test log')
    arg_parser.add_argument('infile', type=argparse.FileType('rt'))
    args = arg_parser.parse_args()
    test_parser = TestParser()
    test_parser.parse(args.infile)
    for (reqid,req) in test_parser.requests.items():
        print(req.as_dict())
    pass
