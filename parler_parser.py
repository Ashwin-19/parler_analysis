from datetime import datetime
from tqdm import tqdm

import ndjson
import os

# start date 
t0 = datetime.strptime("20201101",'%Y%m%d')

# init storage variables
filtered_data = []
user_data = []
usernames = []

# set path to folder with extracted parler_data.zip
for path in os.listdir('user_content_data/'):
    with open('user_content_data/'+path) as f:
       reader = ndjson.reader(f)
       for post in reader:
        if ('createdAt' in post) and ('username' in post) and (datetime.strptime(post["createdAt"],'%Y%m%d%H%M%S') >= t0):
           # hashtags flag
           if ('electionfraud' in post['hashtags']) or ('voterfraud' in post['hashtags']) or ('stopthesteal' in post['hashtags']):
                # delete redundant attributes
                redundant_keys = ['article', 'impressions', 'preview', 'reposts', 'state', 'comments', 'body', 'bodywithurls', 'color', 
                            'controversy', 'createdAtformatted', 'depthRaw', 'lastseents','shareLink','urls']
                for key in redundant_keys:
                    if key in post:
                        del post[key]
                # add to filtered data
                filtered_data.append(post)
                # store username
                usernames.append(post['username'])
    f.close()

with open('filtered_data.ndjson', 'w') as f:
    ndjson.dump(filtered_data, f)

usernames = set(usernames)

# set path to folder with extracted parler_users.zip
for path in os.listdir('user_data/'):
    with open('user_data/'+path) as f:
        reader = ndjson.reader(f)
        for user in reader:
            # user flag
            if ('username' in user) and (user['username'] in usernames):
                user_data.append(user)

with open('user_data.ndjson', 'w') as f:
    ndjson.dump(user_data, f)