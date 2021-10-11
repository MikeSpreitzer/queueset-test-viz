#!/usr/bin/env python3

import argparse
import cairo
import parse_test
import subprocess
import typing


def hue_to_rgb(hue: float, lo: float) -> typing.Tuple[float, float, float]:
    hue = max(0, min(1, hue))
    if hue <= 1/3:
        return (1 - (1-lo)*(hue-0)*3, lo + (1-lo)*(hue-0)*3, lo)
    if hue <= 2/3:
        return (lo, 1-(1-lo)*(hue-1/3)*3, lo + (1-lo)*(hue-1/3)*3)
    return (lo + (1-lo)*(hue-2/3)*3, lo, 1-(1-lo)*(hue-2/3)*3)


def text_in_rectangle(context: cairo.Context, text: str, left: float, top: float, width: float, height: float) -> None:
    extents = context.text_extents(text)
    origin = (left + (width - extents.width)/2 - extents.x_bearing,
              top + (height-extents.height)/2 - extents.y_bearing)
    context.move_to(*origin)
    context.show_text(text)
    return


def render_parse(surface: cairo.Surface, parse: parse_test.TestParser,
                 vert_per_second: float, top_text: str, bottom_text: str) -> None:
    context = cairo.Context(surface)
    context.select_font_face(
        "Sans", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
    num_seats = len(parse.seats)
    num_queues = len(parse.queue_to_lanes)
    hor_per_track = float(36)
    tick_left = float(108)
    seats_left = tick_left + 9
    seats_right = seats_left + hor_per_track * num_seats
    vert_per_header = float(18)
    htop = 0
    if top_text:
        top_text_extents = context.text_extents(top_text)
        htop += vert_per_header
    seats_orig = (seats_left, htop + 2*vert_per_header)
    queues_left = seats_right + hor_per_track
    queues_right = queues_left + hor_per_track * \
        (parse.queue_lane_sum + (num_queues-1) * 0.1)
    page_width = queues_right + hor_per_track*0.5
    if top_text:
        page_width = max(page_width, top_text_extents.width + 24)
    queues_orig = (queues_left, seats_orig[1])
    page_height = seats_orig[1] + \
        (parse.max_t - parse.min_t) * vert_per_second + 1
    if bottom_text:
        bottom_text_extents = context.text_extents(bottom_text)
        bottom_text_orig = (12 - bottom_text_extents.x_bearing,
                            page_height + 6 - bottom_text_extents.y_bearing)
        page_height += bottom_text_extents.height + 12
        page_width = max(
            page_width, bottom_text_orig[0] + bottom_text_extents.x_advance)
    surface.set_size(page_width, page_height)
    print(
        f'num_seats={num_seats}, num_queues={num_queues}, queue_lane_sum={parse.queue_lane_sum}, page_width={page_width}, page_height={page_height}')
    if top_text:
        text_in_rectangle(context, top_text, 0, 0, page_width, vert_per_header)
    if bottom_text:
        context.move_to(*bottom_text_orig)
        context.show_text(bottom_text)
    context.set_line_width(0.5)

    # Render the secion headings
    text_in_rectangle(context, "Seats", seats_left, htop,
                      seats_right-seats_left, vert_per_header)
    text_in_rectangle(context, "Queues", queues_left, htop,
                      queues_right-queues_left, vert_per_header)

    # get ordered list of queues
    qids = sorted([qid for qid in parse.queue_to_lanes])

    # Render the queue headings
    qright = queues_left
    qlefts: typing.Mapping[int, float] = dict()
    htop += vert_per_header
    for qid in qids:
        hleft = qright
        qlefts[qid] = qright
        hwidth = hor_per_track * len(parse.queue_to_lanes[qid].seats)
        qright += hwidth + hor_per_track*0.1
        id_str = str(qid)
        text_in_rectangle(context, id_str, hleft, htop,
                          hwidth, vert_per_header)

    # Render the seat run fills
    num_flows = 1 + parse.max_flow
    for (reqid, req) in parse.requests.items():
        reqid_str = f'{reqid[0]},{reqid[1]},{reqid[2]}'
        stop = seats_orig[1] + vert_per_second * \
            (req.real_dispatch_t-parse.min_t)
        smid = seats_orig[1] + vert_per_second * (req.real_mid_t-parse.min_t)
        sheight1 = vert_per_second*(req.real_mid_t-req.real_dispatch_t)
        sheight2 = vert_per_second*(req.real_finish_t-req.real_mid_t)
        rgb1 = hue_to_rgb(reqid[0]/num_flows, 0.80)
        rgb2 = hue_to_rgb(reqid[0]/num_flows, 0.92)

        context.new_path()
        for (_, run) in enumerate(req.seat_runs1):
            left = seats_orig[0] + run[0]*hor_per_track
            width = run[1]*hor_per_track
            context.rectangle(left, stop, width, sheight1)
        context.set_source_rgb(*rgb1)
        context.fill()
        context.new_path()
        for (_, run) in enumerate(req.seat_runs):
            left = seats_orig[0] + run[0]*hor_per_track
            width = run[1]*hor_per_track
            context.rectangle(left, smid, width, sheight2)
        context.set_source_rgb(*rgb2)
        context.fill()

    context.set_source_rgb(0, 0, 0)

    # Render the rest
    lastick = None
    for (reqid, req) in parse.requests.items():
        reqid_str = f'{reqid[0]},{reqid[1]},{reqid[2]}'
        context.new_path()
        stop = seats_orig[1] + vert_per_second * \
            (req.real_dispatch_t-parse.min_t)
        sheight = vert_per_second*(req.real_finish_t-req.real_dispatch_t)
        if lastick is None or stop > lastick + 18:
            et_str = str(req.real_dispatch_t-parse.min_t)
            text_in_rectangle(context, et_str, 0, stop, seats_left, 0)
            lastick = stop
            context.move_to(tick_left, stop)
            context.line_to(seats_left, stop)

        # Render the seat run outlines
        for (idx, run) in enumerate(req.seat_runs):
            left = seats_orig[0] + run[0]*hor_per_track
            width = run[1]*hor_per_track
            context.rectangle(left, stop, width, sheight)
            if idx == 0:
                label = reqid_str
            else:
                label = reqid_str + chr(97+idx)
            text_in_rectangle(context, label, left, stop, width, sheight)

        # Render the queue entry
        qleft = qlefts[req.queue] + hor_per_track * req.qlane
        qtop = queues_orig[1] + vert_per_second * \
            (req.virt_dispatch_t-parse.min_t)
        qwidth = hor_per_track
        qheight = vert_per_second*(req.virt_finish_t - req.virt_dispatch_t)
        context.rectangle(qleft, qtop, qwidth, qheight)
        text_in_rectangle(context, reqid_str, qleft, qtop, qwidth, qheight)
        context.stroke()
    eval_y = seats_orig[1] + vert_per_second*(parse.eval_t - parse.min_t)
    context.move_to(hor_per_track*0.1, eval_y)
    context.line_to(page_width - hor_per_track*0.1, eval_y)
    context.set_source_rgb(1, 0, 0)
    context.stroke()
    context.show_page()
    return


def git_credit() -> str:
    cp1 = subprocess.run(['git', 'rev-parse', 'HEAD'],
                         capture_output=True, check=True, text=True)
    cp2 = subprocess.run(['git', 'status', '--porcelain'],
                         capture_output=True, check=True, text=True)
    ans = 'Rendered by github.com/MikeSpreitzer/queueset-test-viz commit ' + cp1.stdout.rstrip()
    if cp2.stdout.rstrip():
        ans += ' dirty'
    return ans


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description='render queueset test log')
    arg_parser.add_argument('--vert-per-sec', type=float,
                            default=36, help='points per second, default is 36')
    arg_parser.add_argument('--top-text')
    arg_parser.add_argument(
        '--bottom-text', help='defaults to github reference to renderer')
    arg_parser.add_argument('infile', type=argparse.FileType('rt'))
    arg_parser.add_argument('outfile', type=argparse.FileType('wb'))
    args = arg_parser.parse_args()
    if args.bottom_text is None:
        bottom_text = git_credit()
    else:
        bottom_text = args.bottom_text
    test_parser = parse_test.TestParser()
    test_parser.parse(args.infile)
    surface = cairo.PDFSurface(args.outfile, 100, 100)
    render_parse(surface, test_parser, args.vert_per_sec,
                 args.top_text, bottom_text)
    surface.finish()
    args.outfile.close()
