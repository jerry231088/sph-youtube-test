import requests
from utils.youtube import base_url, api_key


"""Method to get youtube channel's statistics"""


def fetch_channel_statistics(channel_id: str):
    try:
        response = requests.get(
            f"{base_url}channels",
            params={
                'part': 'statistics',
                'id': channel_id,
                'key': api_key
            })
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    except requests.RequestException as e:
        print(f"An error occurred in fetch_channel_statistics: {e}")
        return {}


"""Method to get youtube channel's snippet"""


def fetch_channel_snippet(channel_id: str):
    try:
        response = requests.get(
            url=f"{base_url}channels",
            params={
                'part': 'snippet',
                'id': channel_id,
                'key': api_key
            })
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    except requests.RequestException as e:
        print(f"An error occurred in fetch_channel_snippet: {e}")


"""Method to get youtube channel's status"""


def fetch_channel_status(channel_id: str):
    try:
        response = requests.get(
            url=f"{base_url}channels",
            params={
                'part': 'status',
                'id': channel_id,
                'key': api_key
            })
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    except requests.RequestException as e:
        print(f"An error occurred in fetch_channel_status: {e}")


"""Method to get youtube channel's topicDetails"""


def fetch_channel_topic_categories(channel_id: str):
    try:
        response = requests.get(
            url=f"{base_url}channels",
            params={
                'part': 'topicDetails',
                'id': channel_id,
                'key': api_key
            })
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    except requests.RequestException as e:
        print(f"An error occurred in fetch_channel_topic_categories: {e}")


"""Method to get youtube channel's contentDetails"""


def fetch_channel_content_details(channel_id: str):
    try:
        response = requests.get(
            url=f"{base_url}channels",
            params={
                'part': 'contentDetails',
                'id': channel_id,
                'key': api_key
            })
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    except requests.RequestException as e:
        print(f"An error occurred in fetch_channel_content_details: {e}")


"""Method to get youtube channel's video statistics"""


def fetch_video_statistics(video_id: str):
    try:
        response = requests.get(
            url=f"{base_url}videos",
            params={
                'part': 'statistics',
                'id': video_id,
                'key': api_key
            })
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    except requests.RequestException as e:
        print(f"An error occurred in fetch_video_statistics: {e}")


"""Method to get youtube channel's playlistItems"""


def fetch_play_list_snippet_contents(play_list_id: str, next_page: str):
    try:
        response = requests.get(
            url=f"{base_url}playlistItems",
            params={
                'part': 'snippet,contentDetails',
                'playlistId': play_list_id,
                'maxResults': 50,
                'pageToken': next_page,
                'key': api_key
            })
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    except requests.RequestException as e:
        print(f"An error occurred in fetch_play_list_snippet_contents: {e}")
