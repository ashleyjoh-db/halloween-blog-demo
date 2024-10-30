import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
from PIL import Image

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Define function to run SQL queries
def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# Define function to perform vector search using SQL query
def vectorSearch(query: str) -> pd.DataFrame:
    return sqlQuery(f"SELECT * FROM vector_search(index => 'ashley_johnson.imdb.horror_movies_vs_index', query => '{query}', num_results => 3)")

# Set Streamlit page configuration
st.set_page_config(layout="wide")

# Cache the data to avoid re-querying within 30 seconds
@st.cache_data(ttl=30)
def load_image(image_path):
    return Image.open(image_path)

# Load header image from local directory
image_path = './pic.jpg'
image = load_image(image_path)

# Create columns for layout
col1, col2, col3 = st.columns([1, 1, 2])

# Column 1: Display header and welcome text
with col1:
    st.markdown("<h1 style='font-size: 48px;'>The horror!!!</h1>", unsafe_allow_html=True)
    st.image(image, width=400, use_column_width=False)
    st.write("Welcome to the Horror Movie Database!")
    st.write("Use the search box to find movies similar to your favorite horror movies. Results are based on similarity search of movie plots using Databricks Vector Search.")
    st.write("You can search by keywords or phrases like 'creepy haunted house', 'classic zombie flicks', or 'movies like The Exorcist'.")

# Column 2: Display vertical line for separation
with col2:
    st.markdown(
        """
        <style>
        .vertical-line {
            border-left: 2px solid gray;
            height: 100vh;
            position: absolute;
            left: 50%;
        }
        </style>
        <div class="vertical-line"></div>
        """,
        unsafe_allow_html=True
    )

# Column 3: Display search box and results
with col3:
    st.subheader("Search for movies")
    search = st.text_input("(Example: 'creepy haunted house', 'classic zombie flicks', 'movies like The Exorcist')", value="movies like The Exorcist")

    # Perform vector search and get results
    data = vectorSearch(search)

    # Display results in columns
    cols = st.columns(3)
    for idx, row in data.iterrows():
        col = cols[idx % 3]
        with col:
            st.markdown(f'<a href="{row["wiki_page"]}" target="_blank"><img src="{row["image_url"]}" width="200"></a>', unsafe_allow_html=True)
            st.markdown(f"[{row['title']}]({row['wiki_page']})")
            st.markdown(f"{row['release_year']}")