import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    df['trending_date'] = pd.to_datetime(df['trending_date'], format='%y.%d.%m')
    df['publish_time'] = pd.to_datetime(df['publish_time'])
    df['tags'] = df['tags'].astype("string")
    df['description'] = df['description'].astype("string")
    df['title'] = df['title'].astype("string")
    df['channel_title'] = df['channel_title'].astype("string")
    df['thumbnail_link'] = df['thumbnail_link'].astype("string")
    # Convert to numeric, coerce non-numeric values to NaN
    df['video_id'] = pd.to_numeric(df['video_id'], errors='coerce')

    # Convert NaN values to 0 or handle them based on your requirements
    df['video_id'].fillna(0, inplace=True)

    # Convert the column to integer
    df['video_id'] = df['video_id'].astype(int)


    df = df.drop_duplicates().reset_index(drop=True)
    df['yt_video_id'] = df.index

    df = df.drop_duplicates().reset_index(drop=True)

    publish_time_info = df[['publish_time']].reset_index(drop=True)
    publish_time_info['publish_time'] = publish_time_info['publish_time']
    publish_time_info['publish_hour'] = publish_time_info['publish_time'].dt.hour
    publish_time_info['publish_day'] = publish_time_info['publish_time'].dt.day
    publish_time_info['publish_month'] = publish_time_info['publish_time'].dt.month
    publish_time_info['publish_year'] = publish_time_info['publish_time'].dt.year

    publish_time_info['publish_time_id'] = publish_time_info.index

    publish_time_info = publish_time_info[['publish_time_id', 'publish_time', 'publish_hour', 'publish_day', 'publish_month', 'publish_year']]
    publish_time_info.head()

    channel_info = df[['channel_title']].reset_index(drop=True)
    channel_info['channel_id'] = channel_info.index
    channel_info = channel_info[['channel_id','channel_title']]

    video_status = df[['comments_disabled','ratings_disabled','video_error_or_removed']].reset_index(drop=True)
    video_status['comments_disabled'] = df['comments_disabled']
    video_status['ratings_disabled'] = df['ratings_disabled']
    video_status['video_error_or_removed'] = df['video_error_or_removed']
    video_status['status_id'] = channel_info.index
    video_status = video_status[['status_id','comments_disabled','ratings_disabled','video_error_or_removed']]

    category_type = {
    1:"Film and Animation",
    2:"Autos and Vehicles",
    10:"Music",
    15:"Pets and Animals",
    17:"Sports",
    18:"Short Movies",
    19:"Travel and Events",
    20:"Gaming",
    21:"Videoblogging",
    22:"People and Blogs",
    23:"Comedy",
    24:"Entertainment",
    25:"News and Politics",
    26:"Howto and Style",
    27:"Education",
    28:"Science and Technology",
    29:"Nonprofits and Activism",
    30:"Movies",
    31:"Anime/Animation",
    32:"Action/Adventure",
    33:"Classics",
    34:"Comedy",
    35:"Documentary",
    36:"Drama",
    37:"Family",
    38:"Foreign",
    39:"Horror",
    40:"Sci-Fi/Fantasy",
    41:"Thriller",
    42:"Shorts",
    43:"Shows",
    44:"Trailers"
    }

    video_info = df[['comments_disabled','ratings_disabled','video_error_or_removed']].reset_index(drop=True)
    video_info['title'] = df['title']
    video_info['tags'] = df['tags']
    video_info['thumbnail_link'] = df['thumbnail_link']
    video_info['category_id'] = df['category_id']
    video_info['category_type'] = df['category_id'].map(category_type)
    video_info['title_id'] = video_info.index
    video_info = video_info[['title_id','title','tags','thumbnail_link','category_id','category_type']]

    fact_table = df.merge(publish_time_info, left_on='yt_video_id', right_on='publish_time_id') \
                .merge(video_info, left_on='yt_video_id', right_on='title_id') \
                .merge(channel_info, left_on='yt_video_id', right_on='channel_id') \
                .merge(video_status, left_on='yt_video_id', right_on='status_id') \
                [['yt_video_id','video_id', 'title_id', 'channel_id','status_id', 'publish_time_id','trending_date', 'views', 'likes','dislikes', 'comment_count']]

    # print(fact_table)

    return {
        "video_info":video_info.to_dict(orient="dict"),
        "channel_info":channel_info.to_dict(orient="dict"),
        "publish_time_info":publish_time_info.to_dict(orient="dict"),
        "video_status":video_status.to_dict(orient="dict"),
        "fact_table":fact_table.to_dict(orient="dict")
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
