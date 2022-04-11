import pandas as pd
import json
import time

from igdb.wrapper import IGDBWrapper
wrapper = IGDBWrapper("", "")


'''With a wrapper instance already created'''

# JSON API request

table_names = ["age_rating_content_descriptions", "artworks", "characters",
               "collections", "companies", "external_games", "franchises", "game_engines",
               "game_modes", "game_versions", "game_version_features", "game_version_feature_values",
               "involved_companies", "keywords", "multiplayer_modes", "platforms", 
               "platform_families", "platform_versions", "platform_version_companies",
               "platform_version_release_dates", "player_perspectives", "release_dates"]

for table in table_names:
    print(table)
    game_dicts = []
    offset = 0
    while True:
        print(offset)
        byte_array = wrapper.api_request(f'{table}',f'fields *; offset {offset}; limit 500;')
        # parse into JSON however you like...
        
        if byte_array == b'[]':
            break
        
        game_dicts.append(byte_array)
        
        # for i in games_dict:
        #   print(i)
        
        #print(pd.DataFrame.from_dict(games_dict))
        
        #print(len(games_dict))
        offset += 500
        time.sleep(.5)
        
    df = pd.concat([pd.DataFrame.from_dict(json.loads(game_dict)) for game_dict in game_dicts])
    
    df.to_csv(f"igdb_{table}.csv", index = False)
