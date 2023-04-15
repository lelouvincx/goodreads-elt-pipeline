from contextlib import contextmanager
from datetime import datetime
import streamlit as st
import polars as pl
import psycopg2
from minio import Minio
import os
from PIL import Image
import requests


@contextmanager
def connect_minio():
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )

    try:
        yield client
    except Exception as e:
        raise e


# Make bucket if not exists
def make_bucket(client: Minio, bucket_name):
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists.")


# Download a file from minio
def download_image(book_isbn):
    tmp_file_path = f"/tmp/{book_isbn}_{datetime.today()}.jpeg"
    key_name = f"images/{book_isbn}.jpeg"
    bucket_name = os.getenv("DATALAKE_BUCKET")
    try:
        with connect_minio() as client:
            # Make bucket if not exist
            make_bucket(client=client, bucket_name=bucket_name)
            client.fget_object(bucket_name, key_name, tmp_file_path)
            return tmp_file_path
    except Exception as e:
        raise e


st.set_page_config(
    page_title="Ultimate Goodreads Recommender",
    page_icon="📔",
    layout="centered",
    initial_sidebar_state="expanded",
)


# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


conn = init_connection()


# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


st.title("Ultimate Goodreads recommender!")

book_name = st.text_input("Enter a book name", "Hannibal")
st.write(f"You entered: {book_name}")

# Take isbn from search_prior if it exists, otherwise from search.
isbn = ""
try:
    isbn = run_query(
        f"SELECT isbn FROM recommendations.search_prior WHERE name LIKE '{book_name}' LIMIT 1"
    )[0][0]
    # st.write(f"ISBN: {isbn}")
except IndexError:
    try:
        isbn = run_query(
            f"SELECT isbn FROM recommendations.search WHERE name LIKE '{book_name}' LIMIT 1"
        )[0][0]
        # st.write(f"ISBN: {isbn}")
    except IndexError:
        st.write(f"Book {book_name} not found")
else:
    print("Error while querying book")

# From isbn take list of genreid
genreid = None
if isbn != "":
    try:
        result = run_query(
            f"SELECT genreid FROM gold.book_genre WHERE bookisbn = '{isbn}'"
        )
        genreid = [x[0] for x in result]
        # st.write(f"Genreid: {genreid}")
    except Exception as e:
        conn.commit()
        cursor = conn.cursor()

# From genreid take books with most common genreid
book_with_most_common_genreid = None
if genreid:
    try:
        result = run_query(
            f"""
            WITH common_books AS (
                SELECT bookisbn, COUNT(*) as count
                FROM gold.book_genre
                WHERE genreid IN {tuple(genreid)}
                GROUP BY bookisbn
                HAVING COUNT(*) > 3
                ORDER BY COUNT(*) DESC
            )
            SELECT common_books.bookisbn, common_books.count, criteria.hasdownloadlink, criteria.rating
            FROM recommendations.criteria
            RIGHT JOIN common_books ON common_books.bookisbn = criteria.isbn
            ORDER BY common_books.count DESC, criteria.hasdownloadlink DESC, criteria.rating DESC
            """
        )
        # st.write(result)
        book_with_most_common_genreid = pl.DataFrame(
            {
                "bookisbn": [x[0] for x in result],
                "count": [x[1] for x in result],
                "hasdownloadlink": [x[2] for x in result],
                "rating": [x[3] for x in result],
            }
        )
        book_with_most_common_genreid = book_with_most_common_genreid[:4]
        # st.write(book_with_most_common_genreid)
    except Exception as e:
        print(f"Error while querying: {e}")
        conn.commit()
        cursor = conn.cursor()

# From book_with_most_common_genreid show books
if book_with_most_common_genreid is not None:
    st.subheader(f"You seached for {book_name}, here's information about the book:")
    c1, c2 = st.columns([5, 5])
    rating = book_with_most_common_genreid[0]["rating"][0]
    with c1:
        image = None
        try:
            image = Image.open(download_image(isbn))
            st.image(image, caption=f"{book_name}", width=300)
        except Exception as e:
            req = requests.get(
                f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&jscmd=data&format=json"
            )
            json = req.json()
            image_url = json.get(f"ISBN:{isbn}", {}).get("cover", {}).get("large")
            if image_url:
                image = Image.open(requests.get(image_url, stream=True).raw)
                st.image(image, caption=f"{book_name}", width=300)
    with c2:
        author, language, pagesnumber = "", "", ""
        try:
            result = run_query(
                f"SELECT * FROM gold.book_with_info WHERE isbn = '{isbn}'"
            )
            author = result[0][2]
            language = result[0][3]
            pagesnumber = result[0][5]
        except Exception as e:
            print(f"Error while querying: {e}")
            conn.commit()
            cursor = conn.cursor()
        st.write(f"**Book ISBN**: {isbn}")
        st.write(f"**Book name**: {book_name}")
        if author != "":
            st.write(f"**Author(s)**: {author}")
        if language != "":
            st.write(f"**Language**: {language}")
        if pagesnumber != 0:
            st.write(f"**Pages number**: {pagesnumber}")
        st.write("**Rating**: {:.2f}".format(rating))
        hasdownloadlink = book_with_most_common_genreid[0]["hasdownloadlink"][0]
        if hasdownloadlink:
            if st.button(f"Send {book_name} to kindle", type="primary"):
                st.balloons()

    st.subheader(f"There's {len(book_with_most_common_genreid)-1} related book:")
    for i in range(1, len(book_with_most_common_genreid)):
        c1, c2 = st.columns([5, 5])
        isbn = book_with_most_common_genreid[i]["bookisbn"][0]
        rating = book_with_most_common_genreid[i]["rating"][0]
        with c1:
            book_name, author, language, pagesnumber = "", "", "", ""
            try:
                result = run_query(
                    f"SELECT * FROM gold.book_with_info WHERE isbn = '{isbn}'"
                )
                book_name = result[0][1]
                author = result[0][2]
                language = result[0][3]
                pagesnumber = result[0][5]
            except Exception as e:
                print(f"Error while querying: {e}")
                conn.commit()
                cursor = conn.cursor()
            st.write(f"**Book ISBN**: {isbn}")
            st.write(f"**Book name**: {book_name}")
            if author != "":
                st.write(f"**Author(s)**: {author}")
            if language != "":
                st.write(f"**Language**: {language}")
            if pagesnumber != 0:
                st.write(f"**Pages number**: {pagesnumber}")
                st.write("**Rating**: {:.2f}".format(rating))
            hasdownloadlink = book_with_most_common_genreid[0]["hasdownloadlink"][0]
            if hasdownloadlink:
                if st.button(f"Send {book_name} to kindle", type="primary"):
                    st.balloons()
        with c2:
            image = None
            try:
                image = Image.open(download_image(isbn))
                st.image(image, caption=f"{book_name}", width=300)
            except Exception as e:
                req = requests.get(
                    f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&jscmd=data&format=json"
                )
                json = req.json()
                image_url = json.get(f"ISBN:{isbn}", {}).get("cover", {}).get("large")
                if image_url:
                    image = Image.open(requests.get(image_url, stream=True).raw)
                    st.image(image, caption=f"{book_name}", width=300)
