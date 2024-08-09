import requests
import os
import sys
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)

from utils.s3 import upload_to_s3
from utils.youtube import base_url, api_key, youtube_channels


def is_12_month_old(published_at):
    date_fmt = "%Y-%m-%dT%H:%M:%SZ"
    published_dt = datetime.strptime(published_at, date_fmt)
    one_yr_ago = datetime.now() - relativedelta(months=12)
    return published_dt >= one_yr_ago


def filter_videos(items):
    video_list_12_months_old = []
    for item in items:
        if isinstance(item, dict) and 'snippet' in item:
            snippet = item['snippet']
            if isinstance(snippet, dict) and 'publishedAt' in snippet:
                if is_12_month_old(snippet['publishedAt']):
                    video_list_12_months_old.append(item)
    return video_list_12_months_old


def get_play_list_videos(play_list_id):
    videos = []
    next_page = None

    while True:
        playlist_response = requests.get(
            f"{base_url}playlistItems",
            params={
                'part': 'snippet,contentDetails',
                'playlistId': play_list_id,
                'maxResults': 50,
                'pageToken': next_page,
                'key': api_key
            }
        ).json()
        # append video item only if Item's publishedAt is within 12 months as per current date
        videos.extend(filter_videos(playlist_response.get('items')))
        next_page = playlist_response.get('nextPageToken')
        if not next_page:
            break

    return videos


def get_video_stats(video_id):
    video_response = requests.get(
        f"{base_url}videos",
        params={
            'part': 'statistics',
            'id': video_id,
            'key': api_key
        }
    ).json()
    if len(video_response.get('items', [])) > 0:
        return video_response.get('items', [])[0].get('statistics', {})
    else:
        return {}


def upload_sph_yt_data():
    statistics_data, snippet_data, status_data, topic_category_data, play_list_data = [], [], [], [], []
    for channel_name, channel_id in youtube_channels.items():
        # channel_name, channel_id
        ch_statistics, ch_snippet, ch_status, ch_topic_category, ch_play_list = {}, {}, {}, {}, {}
        init_data = {
            'channel_name': channel_name,
            'channel_id': channel_id
        }
        ch_statistics.update(init_data)

        statistics_response = requests.get(
            f"{base_url}channels",
            params={
                'part': 'statistics',
                'id': channel_id,
                'key': api_key
            }
        ).json()
        statistics = statistics_response.get('items', [])[0].get('statistics', {})
        # viewCount, subscriberCount, hiddenSubscriberCount, videoCount
        ch_statistics.update(
            {
                "viewCount": statistics.get('viewCount'),
                "subscriberCount": statistics.get('subscriberCount'),
                "hiddenSubscriberCount": statistics.get('hiddenSubscriberCount'),
                "videoCount": statistics.get('videoCount')
            }
        )

        statistics_data.append(ch_statistics)
        print(f'{channel_name}: {ch_statistics}')

        ch_snippet.update(init_data)
        snippet_response = requests.get(
            f"{base_url}channels",
            params={
                'part': 'snippet',
                'id': channel_id,
                'key': api_key
            }
        ).json()
        snippet = snippet_response.get('items', [])[0].get('snippet', {})
        ch_snippet.update(
            {
                "title": snippet.get('title'),
                "description": snippet.get('description'),
                "customUrl": snippet.get('customUrl'),
                "publishedAt": snippet.get('publishedAt'),
                "country": snippet.get('country')
            }
        )
        snippet_data.append(ch_snippet)
        print(f'{channel_name}: {ch_snippet}')

        ch_status.update(init_data)
        status_response = requests.get(
            f"{base_url}channels",
            params={
                'part': 'status',
                'id': channel_id,
                'key': api_key
            }
        ).json()
        # privacyStatus, madeForKids, isLinked
        status = status_response.get('items', [])[0].get('status', {})
        ch_status.update(
            {
                "privacyStatus": status.get('privacyStatus'),
                "isLinked": status.get('isLinked'),
                "madeForKids": status.get('madeForKids')
            }
        )
        status_data.append(ch_status)
        print(f'{channel_name}: {ch_status}')

        ch_topic_category.update(init_data)
        topic_category_response = requests.get(
            f"{base_url}channels",
            params={
                'part': 'topicDetails',
                'id': channel_id,
                'key': api_key
            }
        ).json()
        # topicCategories
        topic_category = topic_category_response.get('items', [])[0]
        ch_topic_category.update(
            {
                "topicCategories": topic_category.get('topicDetails', {}).get('topicCategories', [])
            }
        )
        topic_category_data.append(ch_topic_category)
        print(f'{channel_name}: {ch_topic_category}')

        content_detail_response = requests.get(
            f"{base_url}channels",
            params={
                'part': 'contentDetails',
                'id': channel_id,
                'key': api_key
            }
        ).json()
        # playlistId to fetch all videos details from the channel's playlist
        ch_play_list.update(init_data)
        play_list_id = content_detail_response.get('items', [])[0].get('contentDetails', {}).get('relatedPlaylists',
                                                                                                 {}).get('uploads', '')
        ch_play_list.update({'playListId': play_list_id})
        # Fetch only last 12 months video(s) details
        playlist_videos = get_play_list_videos(play_list_id)
        ch_play_list.update({'oneYearOldVideos': playlist_videos})
        play_list_data.append(ch_play_list)
        print(f'{channel_name}: {ch_play_list}')
        # helpful to create trends using above data for all videos like mostViewed or view count, likes, comments, shares?

    part_df_dict = dict()
    statistics_data_df = pd.DataFrame(statistics_data)
    snippet_data_df = pd.DataFrame(snippet_data)
    status_data_df = pd.DataFrame(status_data)
    topic_category_data_df = pd.DataFrame(topic_category_data)
    topic_category_data_df['topicCategories'] = topic_category_data_df['topicCategories'].apply(
        lambda d: d if isinstance(d, list) else [])
    topic_category_data_df = topic_category_data_df.explode('topicCategories')
    topic_category_data_df.reset_index(drop=True, inplace=True)

    play_list_data_df = pd.DataFrame(play_list_data)
    play_list_data_df['oneYearOldVideos'] = play_list_data_df['oneYearOldVideos'].apply(
        lambda d: d if isinstance(d, list) else [])
    play_list_data_df = play_list_data_df.explode('oneYearOldVideos')
    play_list_data_df['oneYearOldVideos'] = play_list_data_df['oneYearOldVideos'].apply(
        lambda d: d if isinstance(d, dict) else {})
    play_list_data_df.reset_index(drop=True, inplace=True)
    play_list_data_df = pd.concat([play_list_data_df, pd.json_normalize(play_list_data_df['oneYearOldVideos'])],
                                  axis=1).drop(columns=['oneYearOldVideos'])
    play_list_data_df['video_stats'] = play_list_data_df['snippet.resourceId.videoId'].apply(
        lambda x: get_video_stats(x))
    play_list_data_df = pd.concat([play_list_data_df, pd.json_normalize(play_list_data_df['video_stats'])],
                                  axis=1).drop(columns=['video_stats'])

    part_df_dict.update(
        {
            'statistics': statistics_data_df,
            'snippet': snippet_data_df,
            'status': status_data_df,
            'topicDetails': topic_category_data_df,
            'playList': play_list_data_df
        }
    )
    for k, v in part_df_dict.items():
        upload_to_s3(v, k)


if __name__ == "__main__":
    upload_sph_yt_data()
