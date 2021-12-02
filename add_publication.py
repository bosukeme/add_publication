import twint
# import nest_asyncio
import os 
import requests
import cv2
import numpy as np
import urlexpander
import urllib.request
import boto3 
import json
from Config.settings import s3_access_key, s3_secret_access_key

BASE_DIR = os.getcwd()

# nest_asyncio.apply

s3 = boto3.client('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_access_key)
s3_resource = boto3.resource('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_access_key)


def get_twitter_handle_bio_details(twitter_handle):
    try:
        c = twint.Config()
        c.Username = twitter_handle
        c.Store_object = True
        c.User_full = False
        c.Pandas =True
        c.Hide_output = True
        twint.run.Lookup(c)
        user_df = twint.storage.panda.User_df.drop_duplicates(subset=['id'])
        try:
            user_id = list(user_df['id'])[0]
        except:
            user_id = 'NA'
        try:
            user_name = list(user_df['name'])[0]
        except:
            user_name = 'NA'
        try:
            user_bio = list(user_df['bio'])[0]
        except:
            user_bio = 'NA'
        try:
            user_profile_image_url = list(user_df['avatar'])[0]
        except:
            user_profile_image_url = 'NA'
        try:
            user_url = list(user_df['url'])[0]
        except:
            user_url = 'NA'
        try:
            user_join_date = list(user_df['join_date'])[0]
        except:
            user_join_date = 'NA'
        try:
            user_location = list(user_df['location'])[0]
        except:
            user_location = 'NA'
        try:
            user_following = list(user_df['following'])[0]
        except:
            user_following = 'NA'
        try:
            user_followers = list(user_df['followers'])[0]
        except:
            user_followers = 'NA'
        try:
            user_verified = list(user_df['verified'])[0]
        except:
            user_verified = 'NA'
    except Exception as e:
        print(e)
        user_name = 'NA'
        user_bio = 'NA'
        user_profile_image_url = 'NA'
        user_url = 'NA'
        user_join_date = 'NA'
        user_location = 'NA'
        user_following = 'NA'
        user_followers = 'NA'
        user_verified = 'NA'

    return user_name, user_bio, user_profile_image_url, user_url, user_location, user_following, user_followers, user_verified



def url_to_image(url, temp_image_path):
    

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}
    
    r = requests.get(url, headers=headers)

    with open(temp_image_path, 'wb') as f:
        f.write(r.content)

    image = cv2.imread(temp_image_path)
    image = cv2.cvtColor(np.uint8(image), cv2.COLOR_BGR2RGB)
    
    return image


def determine_brand_colours(brand_logo_url):
    """
    This function takes in a url for a brand logo image, and then returns the primarly and secondary
    in hexadecimal based on the colours in the logo image
    """
    # Read in the logo image
    temp_article_image_path = os.path.join(BASE_DIR, "temp_article_image.jpg") 
#     print(temp_article_image_path)
    logo_image = url_to_image(brand_logo_url, temp_article_image_path)
    
    # Get frequency of all colours found in the image
    colours, count = np.unique(logo_image.reshape(-1,logo_image.shape[-1]), axis=0, return_counts=True)
    
    # Sort the colours and get the most common non-white colour as the primary colour
    sorted_indices = sorted(range(len(count)), key=lambda k: count[k], reverse=True)
    sorted_indices2 = sorted_indices[0:10]
    sorted_count = [count[i] for i in sorted_indices2]
    sorted_colours = [tuple(colours[i]) for i in sorted_indices2]
    sorted_colours = [colour for colour in sorted_colours if colour != (255, 255, 255)]
    primary_colour = sorted_colours[0]
    
    ## Determine the secondary colour based on how close the primary colour is to being white
    ## For now the secondary colour is either black or white
    white = (255,255,255)
    black = (0,0,0)
    white_diff = np.subtract(white, primary_colour)
    if sum(white_diff) > 150:
        secondary_colour = white
    else:
        secondary_colour = black
        
#     print(primary_colour)
    primary_colour_hex = '#%02x%02x%02x' % primary_colour
#     print(secondary_colour)
    secondary_colour_hex = '#%02x%02x%02x' % secondary_colour
    
    return primary_colour_hex, secondary_colour_hex


def process_shortened_article_url(shortened_url):
    
    try:
        article_url = urlexpander.expand(shortened_url)
        if 'CLIENT_ERROR' in article_url:
            response = requests.get(shortened_url)
            article_url = response.url
        if '__CONNECTIONPOOL_ERROR__' in article_url:
            temp = urllib.request.urlopen(shortened_url)
            article_url = temp.geturl()
        try:
            break_index = article_url.index('?') 
            if break_index > 0:
                article_url = article_url[0:article_url.index('?')]
        except:
            pass
    except Exception as e:
        print(e)
        article_url = 'NA'
    return article_url



def get_bucket_folder_names(bucket_name):
    all_objects = s3.list_objects(Bucket = bucket_name) 
    folder_list = []
    for obj in all_objects['Contents']:
        folder_list.append(obj['Key'])
    return folder_list
def create_folder_in_bucket(bucket_name, folder_name):
    s3.put_object(Bucket=bucket_name, Key=(folder_name+'/'))
def upload_file_to_s3(bucket_name, local_file_path, s3_file_path):
    s3_resource.Bucket(bucket_name).upload_file(local_file_path, s3_file_path)
def download_s3_file(bucket_name, folder_name, file_name, local_output_path):
    s3_file_key = '%s/%s' % (folder_name, file_name)
    s3.download_file(bucket_name, s3_file_key, local_output_path)
def download_twitter_trending_tags(trending_bucket_name, file_name, local_output_path):
    s3.download_file(trending_bucket_name, file_name, local_output_path)



def upload_to_s3_bucket(brand_details_dict, twitter_handle):

    local_file_path = os.path.join(BASE_DIR, 'temp.json')
    brand_id = twitter_handle
    s3_file_path = '%s/brand_details.json' % brand_id
    bucket_name = "bloverse-test-brands"

    with open(local_file_path, 'w') as fp:
        json.dump(brand_details_dict, fp)

    try:
        upload_file_to_s3(bucket_name, local_file_path, s3_file_path)
    except Exception as e:
        print(e)



def process_brand_details(handle, user_name, domain_url, user_profile_image_url, primary_colour_hex, secondary_colour_hex, rss_link):
    brand_details_dict = {'brand_bucket_name': 'bloverse-test-brands',
                         'brand_id': handle,
                         'brand_name': user_name,
                         'brand_domain_url': domain_url,
                         'brand_url_text': f'For more on this post visit us at {domain_url}',
                         'article_tweets_have_urls': False,
                         'brand_logo_url': user_profile_image_url,
                         'brand_font_name': 'OpenSans-ExtraBold.ttf',
                         'brand_primary_colour': primary_colour_hex, 
                         'brand_secondary_colour': secondary_colour_hex,
                         'rss_link': rss_link}
    
    return brand_details_dict



def add_new_publication(twitter_handle, rss_link):

    user_name, user_bio, user_profile_image_url, user_url, user_location, user_following, user_followers, user_verified = get_twitter_handle_bio_details(twitter_handle)
    primary_colour_hex, secondary_colour_hex = determine_brand_colours(user_profile_image_url)
    domain_url = process_shortened_article_url(user_url)
    brand_details_dict = process_brand_details(twitter_handle, user_name, domain_url, user_profile_image_url, primary_colour_hex, secondary_colour_hex, rss_link)
    upload_to_s3_bucket(brand_details_dict, twitter_handle)
    
    return brand_details_dict


# twitter_handle = "technodechina"
# add_new_publication(twitter_handle)

