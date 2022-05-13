import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy import MetaData
from sqlalchemy.sql import text


class DataLoader():

    def __init__(self, filepath:str, index = None) -> None:
        """
        Loads a CSV file path into a Dataframe

        Args:
            filepath (str): file path to the CSV file
            index: default to none
        """
        df = pd.read_csv(filepath, header = 0)
        self.df = df

    def head(self) -> None:
        """
        prints the head of the dataframe to console
        """
        return self.df.head()

    def add_index(self, index_name:str, column_names:list) -> None:
        """
        Create a dataframe index column from concatenating a series of column values. Column values are concatenated by a dash "-".

        For example if you have three columns such as: artist="Metallica", song="Ride the Lighting"
        the index would be ""Metallica-Ride the Lighting"

        Args:
            index_name (str): the index column name
            colum_names (list): list of columns to concatenate into an index column
        """
        df = self.df
        df[index_name] = df[column_names].apply(lambda row: "-".join(row.values.astype(str)), axis=1)
        df.set_index(index_name, inplace= True)
        self.df = df

    def sort(self, column_name:str) -> None:
        """
        Sorts the dataframe by a particular column

        Args:
            column_name (str): column name to sort by
        """
        return self.df.sort_values(column_name, axis = 0, ascending = True, inplace = False)

    def load_to_db(self, db_engine, db_table_name:str) -> None:
        """
        Loads the dataframe into a database table.

        Args:
            db_engine (SqlAlchemy Engine): SqlAlchemy engine (or connection) to use to insert into database
            db_table_name (str): name of database table to insert to
        """
        #
        self.df.to_sql(name=db_table_name, con=self.engine, if_exists='append', chunksize=2000)





def db_engine(db_host:str, db_user:str, db_pass:str, db_name:str="spotify") -> sa.engine.Engine:
    """Using SqlAlchemy, create a database engine and return it

    Args:
        db_host (str): datbase host and port settings
        db_user (str): database user
        db_pass (str): database password
        db_name (str): database name, defaults to "spotify"

    Returns:
        sa.engine.Engine: sqlalchemy engine
    """
    #create enginge
    engine = create_engine(f'mysql+pymsql://{db_user}:{db_pass}@{db_host}/{db_name}', future = True)
    metadata = MetaData(bind=engine)
    conn = engine.connect()
    return engine

def db_create_tables(db_engine, drop_first:bool = False) -> None:
    """
    Using SqlAlchemy Metadata class create two spotify tables (including their schema columns and types)
    for **artists** and **albums**.


    Args:
        db_engine (SqlAlchemy Engine): SqlAlchemy engine to bind the metadata to.
        drop_first (bool): Drop the tables before creating them again first. Default to False
    """
    meta = sa.MetaData(bind=db_engine)

    #define tables
    artists_table = Table("artists",
        metadata,
        Column('index', Numeric),
        Column('artist_poularity', Numeric),
        Column('followers', Numeric),
        Column('genres', String(10240)),
        Column('id', Numeric, primary_key=True),
        Column('name', String(256)),
        Column('track_id', String(256)),
        Column('track_name_prev', String(256)),
        Column('type', String(256)),
        extend_existing=True
    )

    albums_table = Table("albums",
        metadata,
        Column('index', Numeric),
        Column('album_type', String(256)),
        Column('artist_id', String(256)),
        Column('available_markets', String(10240)),
        Column('external_urls', String(256)),
        Column('href', String(256)),
        Column('id', String(256), primary_key=True),
        Column('name', String(10240)),
        Column('release_date', DateTime, nullable=True),
        Column('release_date_precision', String(256)),
        Column('total_tracks', Numeric),
        Column('track_id', String(256)),
        Column('track_name_prev', String(256)),
        Column('uri', String(256)),
        Column('type', String(256)),
        extend_existing=True
    )

    #drop tables is drop_first = True
    if drop_first:
        metadata.drop_all()
    
    #create tables
    metadata.create_all(checkfirst=True)


def main():
    """
    Pipeline Orchestratation method.

    Performs the following:
    - Creates a DataLoader instance for artists and albums
    - prints the head for both instances
    - Sets artists index to id column
    - Sets albums index to artist_id, name, and release_date
    - Sorts artists by name
    - creates database engine
    - creates database metadata tables/columns
    - loads both artists and albums into database
    """
    artist_loader = DataLoader('./data/spotify_artists.csv', index = 'id')
    album_loader = DataLoader('./data/spotify_albums.csv')
    artist_loader.head()
    album_loader.head()
    album_loader.add_index('index', ['artist_id','name','release_date'])
    artist_loader.sort('name')
    engine = db_engine('127.0.0.1:3306', 'root', 'mysql')
    db_create_tables(engine, drop_first = True)
    artist_loader.load_to_db(engine, 'artists')
    album_loader.load_to_db(engine, 'albums')
    

if __name__ == '__main__':
    main()