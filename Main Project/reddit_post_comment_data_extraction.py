import os
import datetime
import pandas as pd
from psaw import PushshiftAPI

api = PushshiftAPI()

df_info = pd.read_csv("./Data/Reddit Info.csv")
count = 0
for row in df_info.iterrows():
    row = row[1]
    if row["Checked?"] == "N":
        break
    if row["Subreddit?"] == "N":
        continue
    if row["Info"] == "" or type(row["Info"]) == float:
        continue
    
    game_name = row["Title"]
    count += 1
    print(count, game_name)
    
    df_checked = pd.read_csv("./Data/checked_games.csv")
    
    if game_name in list(df_checked["Title"]):
        print("Already extracted")
        continue
    
    subreddit = row["Info"]
    epoch_time = datetime.datetime.strptime(row["Release_Date"], '%m/%d/%Y').strftime("%S")


    comments = list(api.search_comments(subreddit=subreddit, 
                                        limit=1000, 
                                        after=epoch_time,
                                        sort='created_utc:asc'))
    
    
    #print(len(comments[0]))
    
    cols = ['game_name', 'all_awardings',
     'archived',
     'associated_award',
     'author',
     'author_flair_background_color',
     'author_flair_css_class',
     'author_flair_richtext',
     'author_flair_template_id',
     'author_flair_text',
     'author_flair_text_color',
     'author_flair_type',
     'author_fullname',
     'author_patreon_flair',
     'author_premium',
     'body',
     'body_sha1',
     'can_gild',
     'collapsed',
     'collapsed_because_crowd_control',
     'collapsed_reason',
     'collapsed_reason_code',
     'comment_type',
     'controversiality',
     'count',
     'created',
     'created_utc',
     'd_',
     'distinguished',
     'gilded',
     'gildings',
     'id',
     'index',
     'is_submitter',
     'link_id',
     'locked',
     'no_follow',
     'parent_id',
     'permalink',
     'retrieved_utc',
     'score',
     'score_hidden',
     'send_replies',
     'stickied',
     'subreddit',
     'subreddit_id',
     'subreddit_name_prefixed',
     'subreddit_type',
     'top_awarded_type',
     'total_awards_received',
     'treatment_tags',
     'unrepliable_reason']
    
    dict1 = {"game_name": game_name}
    comments = [{**dict1, **i[-1]} for i in comments]
    
    df_new = pd.DataFrame(columns=cols)
    
    df_new = df_new.append(pd.DataFrame.from_dict(comments))
    
    if os.path.isfile("./Data/reddit_comments.csv"):
        df_main = pd.read_csv("./Data/reddit_comments.csv")
        df_new = df_main.append(df_new, ignore_index=True)
    
    df_new.to_csv("./Data/reddit_comments.csv", index=False)
    
    df_new_checked = pd.DataFrame([game_name], columns=['Title'])
    df_checked = df_checked.append(df_new_checked, ignore_index=True)
    
    df_checked.to_csv("./Data/checked_games.csv", index=False)
