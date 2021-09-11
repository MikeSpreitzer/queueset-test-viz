#!/usr/bin/env python3

import argparse
import cairo
import parse_test
import typing


def hue_to_rgb(hue: float) -> typing.Tuple[float, float, float]:
    lo = float(0.9)
    hue = max(0, min(1, hue))
    if hue <= 1/3:
        return (1 - (1-lo)*(hue-0)*3, lo + (1-lo)*(hue-0)*3, lo)
    if hue <= 2/3:
        return (lo, 1-(1-lo)*(hue-1/3)*3, lo + (1-lo)*(hue-1/3)*3)
    return (lo + (1-lo)*(hue-2/3)*3, lo, 1-(1-lo)*(hue-2/3)*3)


def text_in_rectangle(context: cairo.Context, text: str, left: float, top: float, width: float, height: float) -> None:
    extents = context.text_extents(text)
    context.move_to(left + (width - extents.width)/2 - extents.x_bearing,
                    top + (height-extents.height)/2 - extents.y_bearing)
    context.show_text(text)
    return


def render_parse(surface: cairo.Surface, parse: parse_test.TestParser, vert_per_second: float) -> None:
    context = cairo.Context(surface)
    num_seats = len(parse.seats)
    num_queues = len(parse.queue_to_lanes)
    hor_per_track = float(36)
    seats_left = hor_per_track*0.5
    seats_right = seats_left + hor_per_track * num_seats
    vert_per_header = float(18)
    seats_orig = (seats_left, 2*vert_per_header)
    queues_left = seats_right + hor_per_track
    queues_right = queues_left + hor_per_track * \
        (parse.queue_lane_sum + (num_queues-1) * 0.1)
    page_width = queues_right + hor_per_track*0.5
    queues_orig = (queues_left, seats_orig[1])
    page_height = seats_orig[1] + (parse.max_t - parse.min_t) * vert_per_second
    surface.set_size(page_width, page_height)
    print(
        f'num_seats={num_seats}, num_queues={num_queues}, queue_lane_sum={parse.queue_lane_sum}, page_width={page_width}, page_height={page_height}')
    context.set_line_width(0.5)
    context.select_font_face(
        "Sans", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)

    # Render the secion headings
    htop = 0
    hheight = vert_per_header
    text_in_rectangle(context, "Seats", seats_left, 0,
                      seats_right-seats_left, vert_per_header)
    text_in_rectangle(context, "Queues", queues_left, 0,
                      queues_right-queues_left, vert_per_header)

    # Render the queue headings
    qright = queues_left
    qlefts: typing.Mapping[int, float] = dict()
    htop = vert_per_header
    for qid in parse.queue_to_lanes:
        hleft = qright
        qlefts[qid] = qright
        hwidth = hor_per_track * len(parse.queue_to_lanes[qid].seats)
        qright += hwidth + hor_per_track*0.1
        id_str = str(qid)
        text_in_rectangle(context, id_str, hleft, htop, hwidth, hheight)

    # Render the seat run fills
    num_flows = 1 + parse.max_flow
    for (reqid, req) in parse.requests.items():
        reqid_str = f'{reqid[0]},{reqid[1]},{reqid[2]}'

        for (idx, run) in enumerate(req.seat_runs):
            left = seats_orig[0] + run[0]*hor_per_track
            top = seats_orig[1] + vert_per_second * \
                (req.real_dispatch_t-parse.min_t)
            width = run[1]*hor_per_track
            height = vert_per_second*(req.real_finish_t-req.real_dispatch_t)
            context.new_path()
            context.rectangle(left, top, width, height)
            context.set_source_rgb(*hue_to_rgb(reqid[0]/num_flows))
            context.fill()

    context.set_source_rgb(0, 0, 0)

    for (reqid, req) in parse.requests.items():
        reqid_str = f'{reqid[0]},{reqid[1]},{reqid[2]}'
        context.new_path()

        # Render the seat run outlines
        for (idx, run) in enumerate(req.seat_runs):
            left = seats_orig[0] + run[0]*hor_per_track
            top = seats_orig[1] + vert_per_second * \
                (req.real_dispatch_t-parse.min_t)
            width = run[1]*hor_per_track
            height = vert_per_second*(req.real_finish_t-req.real_dispatch_t)
            context.rectangle(left, top, width, height)
            if idx == 0:
                label = reqid_str
            else:
                label = reqid_str + chr(97+idx)
            text_in_rectangle(context, label, left, top, width, height)

        # Render the queue entry
        qleft = qlefts[req.queue] + hor_per_track * req.qlane
        qtop = queues_orig[1] + vert_per_second * \
            (req.virt_dispatch_t-parse.min_t)
        qwidth = hor_per_track
        qheight = vert_per_second*(req.virt_finish_t - req.virt_dispatch_t)
        if reqid[0] == 0 and reqid[1] == 0:
            print(f'q rect = {(qleft, qtop, qwidth, qheight)}')
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


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        description='render queueset test log')
    arg_parser.add_argument('--vert-per-sec', type=float, default=36)
    arg_parser.add_argument('infile', type=argparse.FileType('rt'))
    arg_parser.add_argument('outfile', type=argparse.FileType('wb'))
    args = arg_parser.parse_args()
    test_parser = parse_test.TestParser()
    test_parser.parse(args.infile)
    surface = cairo.PDFSurface(args.outfile, 100, 100)
    render_parse(surface, test_parser, args.vert_per_sec)
    surface.finish()
    args.outfile.close()
