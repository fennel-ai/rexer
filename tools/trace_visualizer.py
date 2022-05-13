""" Speedscope recorder for rexer tracers.

See: https://github.com/jlfwong/speedscope/wiki/Importing-from-custom-sources
"""

import argparse
import time
import json
import sys
from collections import namedtuple
import contextlib
from typing import List

parser = argparse.ArgumentParser()
parser.add_argument("--trace-path", help="path to the json file with the trace")
args = parser.parse_args()
path = args.trace_path


def main():
    # Parse the json file
    with open(path) as trace_src:
        t = trace_src.read().replace('\t', '')
        t = t.replace('\n', '')
        data = json.loads(t)

    # Fetch `msg` field value;
    # Parse it to fetch each entry
    #   1. parse the string removing newlines, tabs and whitespaces
    #   2. first entry is `====Trace====` - skip this
    #   3. last entry has `\n` at the end, remove that
    events = data['msg'].split('\n')[1:]
    formatted_events = []
    for e in events:
        formatted_events.append(e.strip())
    formatted_events = formatted_events[:-1]

    # Each event is of the format - `388ms: exit:vEWWxe:redis.mget`
    # For each entry/record
    #   1. Create a "frame" -> trace entry id and create an index to it
    #   2. Log each record as an event. Event has the following format:
    #       {"type": "C" or "O", "at": timestamp (elapsed timestamp for us), "frame": frame_index, "name": method_name}
    events = []
    frames = []
    frame_cache = {}
    types = set()
    stack = []
    for e in formatted_events:
        attrs = e.split(':')
        timestamp = int(attrs[0][:-2])
        a = attrs[1].strip()
        id = attrs[2]
        name = attrs[3]
        types.add(a)
        type = "C" if a == "exit" else "O"
        if a == "enter":
            stack.append((id, name))
        else:
            id_, name_ = stack.pop()
            if id_ != id or name_ != name:
                print(f'found an unmatched exit: {id_} v/s {id}, {name_} v/s {name}, current stack: {stack} \n')
        if id not in frame_cache:
            frame_cache[id] = len(frames)
            frames.append({"name": name})
        idx = frame_cache[id]
        events.append({"type": type, "at": timestamp, "frame": idx})

    print(f'unique types: {types}\n')
    # export the formatted json and ask to upload to `https://www.speedscope.app/`
    data = {
        "$schema": "https://www.speedscope.app/file-format-schema.json",
        "profiles": [
            {
                "type": "evented",
                "name": "python",
                "unit": "milliseconds",
                "startValue": 0,
                "endValue": events[-1]["at"],
                "events": events,
            }
        ],
        "shared": {"frames": frames},
        "activeProfileIndex": 0,
        "exporter": "trace_visualizer",
        "name": "profile for rexer trace",
    }
    print('Writing formatted data to speedscope_trace.json, please upload it to https://www.speedscope.app/!')
    with open('speedscope_trace.json', 'w') as f:
        json.dump(data, f)


if __name__ == "__main__":
    main()