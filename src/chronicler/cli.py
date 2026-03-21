"""CLI entry points for Chronicler."""

from __future__ import annotations

import argparse
import sys


def main(argv=None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(prog='chronicler', description='Chronicler event collector')
    subparsers = parser.add_subparsers(dest='command')

    # start
    start_parser = subparsers.add_parser('start', help='Start the Chronicler engine')
    start_parser.add_argument('--config', default='chronicler.yaml', help='Config file path')

    # status
    status_parser = subparsers.add_parser('status', help='Show engine status')
    status_parser.add_argument('--state-file', default='chronicler_state.jsonl')

    # stories
    stories_parser = subparsers.add_parser('stories', help='Story management')
    stories_sub = stories_parser.add_subparsers(dest='stories_command')
    list_parser = stories_sub.add_parser('list', help='List stories')
    list_parser.add_argument('--filter', choices=['open', 'closed', 'all'], default='all')
    list_parser.add_argument('--state-file', default='chronicler_state.jsonl')
    show_parser = stories_sub.add_parser('show', help='Show story detail')
    show_parser.add_argument('story_id', help='Story ID')
    show_parser.add_argument('--state-file', default='chronicler_state.jsonl')

    # replay
    replay_parser = subparsers.add_parser('replay', help='Replay JSONL events')
    replay_parser.add_argument('file', help='JSONL file to replay')
    replay_parser.add_argument('--config', default='chronicler.yaml')
    replay_parser.add_argument('--suppress-sinks', action='store_true')

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    return 0


if __name__ == '__main__':
    sys.exit(main())
