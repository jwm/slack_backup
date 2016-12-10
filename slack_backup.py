#!/usr/bin/env python

# Copyright (c) 2016 Chandler Abraham
# Copyright (c) 2016 John Morrissey <jwm@horde.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import argparse
import json
import os
from tempfile import NamedTemporaryFile

from slacker import Slacker


SORT_MESSAGES_BY_TIMESTAMP = lambda message: float(message['ts'])


def _get_stored_messages(path):
    if not os.path.exists(path):
        return []

    with open(path) as stored_file:
        return json.load(stored_file)['messages']


def _get_max_timestamp(messages):
    try:
        return max([
            float(message['ts']) for message in messages
        ])
    except ValueError:
        return 0.


def get_history(
        pageable_object, channel_id, start_at_timestamp=0,
        page_size=1000
):
    """
    Fetch the complete message history for a channel/group/direct message.
    """
    messages = []

    while True:
        response = pageable_object.history(
            channel=channel_id,
            oldest=int(start_at_timestamp),
            count=page_size,
        ).body

        messages.extend([
            message
            for message
            in response['messages']
            if float(message['ts']) > start_at_timestamp
        ])

        if not response['has_more']:
            break
        start_at_timestamp = _get_max_timestamp(response['messages'])

    print('{} new messages.'.format(len(messages)))
    messages.sort(key=SORT_MESSAGES_BY_TIMESTAMP)
    return messages


def _write_json(path, info):
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with NamedTemporaryFile(
            dir=directory, mode='w', delete=False) as out_file:
        json.dump(info, out_file, indent=4, sort_keys=True)
        os.rename(out_file.name, path)


def _append_new_messages(slack_api_handle, item_id, output_path, info):
    messages = _get_stored_messages(output_path)
    new_messages = get_history(
        slack_api_handle, item_id, _get_max_timestamp(messages)
    )
    if not new_messages:
        return

    messages += new_messages
    messages.sort(key=SORT_MESSAGES_BY_TIMESTAMP)
    info['messages'] = messages

    _write_json(output_path, info)


def get_channels(slack):
    """Write history for all public channels."""
    channels = slack.channels.list().body['channels']

    print('\nChannels:')
    for channel in channels:
        print('  #{}: '.format(channel['name']), end='')

        output_path = 'channels/{}.json'.format(channel['name'])
        info = {
            'channel_info': (
                slack.channels.info(channel['id']).body['channel']
            )
        }
        _append_new_messages(
            slack.channels, channel['id'], output_path, info
        )


def get_direct_messages(slack, owner_id, user_map):
    """
    Write history for all direct message conversations ("IMs" in the
    slack API).
    """
    def _get_dm_name(dm):
        return user_map.get(
            dm['user'], '{} (name unknown)'.format(dm['user'])
        )

    dms = slack.im.list().body['ims']
    dms.sort(key=_get_dm_name)

    print('\nDirect messages:')
    for dm in dms:
        name = _get_dm_name(dm)
        print('  {}: '.format(name), end='')

        output_path = 'direct_messages/{}.json'.format(name)
        info = {
            'channel_info': {
                'members': [dm['user'], owner_id],
            },
        }
        _append_new_messages(slack.im, dm['id'], output_path, info)


def get_private_channels(slack):
    """
    Fetch and write history for all private channels ("groups" in the
    Slack API).
    """
    groups = slack.groups.list().body['groups']

    print('\nPrivate channels:')
    for group in groups:
        print('  #{}: '.format(group['name']), end='')

        output_path = 'private_channels/{}.json'.format(group['name'])
        info = {
            'channel_info': slack.groups.info(group['id']).body['group'],
        }
        _append_new_messages(slack.groups, group['id'], output_path, info)


def get_team_info(slack):
    auth_info = slack.auth.test().body
    users = slack.users.list().body['members']

    print('Authenticated to team {} ({} users) as {}.'.format(
        auth_info['team'], len(users), auth_info['user']
    ))

    user_map = {
        user['id']: user['name']
        for user
        in users
    }

    return auth_info, user_map


def main():
    parser = argparse.ArgumentParser(
        description="Back up a Slack team's message history"
    )

    parser.add_argument('--token', help="a team user's API token")

    parser.add_argument(
        '-P', '--skip-private-channels', action='store_true', default=False,
        help="don't fetch history for private channels")
    parser.add_argument(
        '-C', '--skip-channels', action='store_true', default=False,
        help="don't fetch history for channels")
    parser.add_argument(
        '-M', '--skip-direct-messages', action='store_true', default=False,
        help="don't fetch history for directMessages")

    args = parser.parse_args()

    slack = Slacker(args.token)
    auth_info, user_map = get_team_info(slack)

    print('Writing metadata...')
    metadata = {
        'auth_info': auth_info,
        'users': user_map,
    }
    _write_json('./metadata.json', metadata)

    if not args.skip_channels:
        get_channels(slack)

    if not args.skip_private_channels:
        get_private_channels(slack)

    if not args.skip_direct_messages:
        get_direct_messages(slack, auth_info['user_id'], user_map)


if __name__ == '__main__':
    main()
