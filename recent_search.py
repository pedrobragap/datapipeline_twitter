import requests
import os
import json

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")

search_url = "https://api.twitter.com/2/tweets/search/recent"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
query_params = {'query': 'AluraOnline',
                'tweet.fields': 'author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text',
                'expansions':'author_id', 
                'user.fields':'id,name,username,created_at',
                'start_time':'2021-09-08T00:00:00Z',
                'end_time':'2021-09-11T00:00:00Z'}

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def paginete(search_url,query_params, next_token=""):
    if next_token:
        query_params['next_token'] = next_token

    data = connect_to_endpoint(search_url, query_params)
    yield data
    if "next_token" in data.get("meta",{}):
        yield from paginete(search_url, query_params, data['meta']['next_token'])


def main():
    #json_response = connect_to_endpoint(search_url, query_params)
    for json_response in paginete(search_url,query_params):
        print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()