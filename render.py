#!/usr/bin/env python3

import argparse
import cairo
import parse_test
import typing


def render_parse(surface: cairo.Surface, parse: parse_test.TestParser, vert_per_second: float) -> None:
    context = cairo.Context(surface)
    num_seats = len(parse.seats)
    num_queues = len(parse.queue_positions)
    hor_per_track = float(36)
    seat_orig = (hor_per_track*0.5, vert_per_second*1.1)
    queue_orig = (hor_per_track * (num_seats + 1.5), seat_orig[1])
    page_width = queue_orig[0] + hor_per_track * \
        (parse.queue_lane_sum + num_queues * 0.1 + 0.4)
    page_height = seat_orig[1] + (parse.max_t - parse.min_t) * vert_per_second
    surface.set_size(page_width, page_height)
    print(
        f'num_seats={num_seats}, num_queues={num_queues}, queue_lane_sum={parse.queue_lane_sum}, page_width={page_width}, page_height={page_height}')
    context.set_line_width(0.5)
    context.select_font_face(
        "Sans", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
    qright = queue_orig[0]
    qlefts: typing.Mapping[int, float] = dict()

    for (qid, qpos) in parse.queue_positions.items():
        hleft = qright
        qlefts[qid] = qright
        hwidth = hor_per_track * len(parse.queue_to_lanes[qid].seats)
        qright +=  hwidth + hor_per_track*0.1
        id_str = str(qid)
        extents = context.text_extents(id_str)
        htop = 0
        hheight = vert_per_second
        context.move_to(hleft + (hwidth - extents.width)/2 - extents.x_bearing,
                        htop + (hheight-extents.height)/2 - extents.y_bearing)
        context.show_text(id_str)

    for (reqid, req) in parse.requests.items():
        context.new_path()
        reqid_str = f'{reqid[0]},{reqid[1]},{reqid[2]}'
        for (idx, run) in enumerate(req.seat_runs):
            left = seat_orig[0] + run[0]*hor_per_track
            top = seat_orig[1] + vert_per_second * \
                (req.real_dispatch_t-parse.min_t)
            width = run[1]*hor_per_track
            height = vert_per_second*(req.real_finish_t-req.real_dispatch_t)
            context.rectangle(left, top, width, height)
            if idx == 0:
                label = reqid_str
            else:
                label = reqid_str + chr(97+idx)
            extents = context.text_extents(label)
            context.move_to(left + (width - extents.width)/2 - extents.x_bearing,
                            top + (height-extents.height)/2 - extents.y_bearing)
            context.show_text(label)
        qpos = parse.queue_positions[req.queue]
        qleft = qlefts[req.queue] + hor_per_track * req.qlane
        qtop = queue_orig[1] + vert_per_second * \
            (req.virt_dispatch_t-parse.min_t)
        qwidth = hor_per_track
        qheight = vert_per_second*(req.virt_finish_t - req.virt_dispatch_t)
        if reqid[0] == 0 and reqid[1] == 0:
            print(f'q rect = {(qleft, qtop, qwidth, qheight)}')
        context.rectangle(qleft, qtop, qwidth, qheight)
        extents = context.text_extents(reqid_str)
        context.move_to(qleft + (qwidth - extents.width)/2 - extents.x_bearing,
                        qtop + (qheight-extents.height)/2 - extents.y_bearing)
        context.show_text(reqid_str)
        context.stroke()
    eval_y = seat_orig[1] + vert_per_second*(parse.eval_t - parse.min_t)
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
