from re import A, search
from airflow.hooks.http_hook import HttpHook
import requests
import json

class TwitterHook(HttpHook):

    def __init__(self, query, conn_id = None, start_time = None, end_time=None):
        self.query = query
        self.conn_id = conn_id or "twitter_default"
        self.start_time = start_time
        self.end_time = end_time

        super().__init__(http_conn_id=self.conn_id)
    
    def create_url(self):
        self.search_url = f"{self.base_url}/2/tweets/search/recent"

        # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
        # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
        self.query_params = {'query': self.query,
                        'tweet.fields': 'author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text',
                        'expansions':'author_id', 
                        'user.fields':'id,name,username,created_at',
                        }
        if self.start_time:
            self.query_params['start_time'] = self.start_time
        
        if self.end_time:
            self.query_params['end_time'] = self.end_time

        return self.search_url, self.query_params
        


    def connect_to_endpoint(self, url, params, session):
        response = requests.Request("GET",url, params=params)
        prep = session.prepare_request(response)
        self.log.info(f"URL:{url}\nParams: {params}")
        return self.run_and_check(session,prep, {}).json()

    def paginete(self, search_url, session ,query_params, next_token=""):
        if next_token:
            query_params['next_token'] = next_token

        data = self.connect_to_endpoint(search_url, query_params, session)
        yield data
        if "next_token" in data.get("meta",{}):
            yield from self.paginete(search_url, session,query_params, data['meta']['next_token'])

    
    

    def run(self):
        session = self.get_conn()
        search_url,query_params = self.create_url()
        yield from self.paginete(search_url, session ,query_params)


if __name__ == "__main__":
    for pg in TwitterHook("AluraOnline").run():
        print(json.dumps(pg, indent=4, sort_keys=4))